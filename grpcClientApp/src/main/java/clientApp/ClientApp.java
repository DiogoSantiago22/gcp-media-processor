package clientApp;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import pt.isel.cn2026.grpc.*; // Importa as classes geradas pelo contrato
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.CountDownLatch;
import java.time.LocalDate;
import java.time.format.DateTimeParseException;
import pt.isel.cn2026.grpc.FuncionalidadesGeraisGrpc.FuncionalidadesGeraisBlockingStub;
import pt.isel.cn2026.grpc.FuncionalidadesGeraisGrpc.FuncionalidadesGeraisStub;
import pt.isel.cn2026.grpc.GestaoElasticidadeGrpc.GestaoElasticidadeBlockingStub;

public class ClientApp {
    private static String svcIP;
    private static int svcPort = 8000;

    // Stubs para o nosso sistema
    private static FuncionalidadesGeraisBlockingStub blockingStub;
    private static FuncionalidadesGeraisStub noBlockStub;
    private static GestaoElasticidadeBlockingStub gestaoStub;

    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);
        ManagedChannel channel;

        // Descoberta dos IPs das VMs disponiveis no grupo de instâncias dos servidores
        while (true) {
            try {
                List<String> ipsDisponiveis = obterIps();

                if (ipsDisponiveis.isEmpty()) {
                    System.out.println("Nenhum servidor ativo foi encontrado pelo grupo de instâncias.");
                    System.out.println("A aguardar 5 segundos para tentar novamente...");
                    Thread.sleep(5000);
                    continue;
                }

                System.out.println("\n--- Servidores gRPC Disponíveis ---");
                for (int i = 0; i < ipsDisponiveis.size(); i++) {
                    System.out.println((i + 1) + " - " + ipsDisponiveis.get(i));
                }
                System.out.println("0 - Atualizar lista de servidores");

                String entrada = read("Escolha uma opção para estabelecer ligação:", scanner);
                int escolha = Integer.parseInt(entrada);

                if (escolha == 0) {
                    continue; // Repete o lookup para atualizar a lista
                }

                if (escolha > 0 && escolha <= ipsDisponiveis.size()) {
                    svcIP = ipsDisponiveis.get(escolha - 1);
                    System.out.println("A estabelecer ligação a " + svcIP + ":" + svcPort + "...");

                    // Configuração e inicialização do Canal gRPC
                    channel = ManagedChannelBuilder.forAddress(svcIP, svcPort)
                            .usePlaintext()
                            .build();

                    blockingStub = FuncionalidadesGeraisGrpc.newBlockingStub(channel);
                    noBlockStub = FuncionalidadesGeraisGrpc.newStub(channel);
                    gestaoStub = GestaoElasticidadeGrpc.newBlockingStub(channel);

                    break; // Ligação bem sucedida sair do ciclo
                } else {
                    System.out.println("Opção inválida. Tente novamente.");
                }
            } catch (Exception e) {
                System.out.println("Falha ao obter servidores ou na ligação: " + e.getMessage());
                System.out.println("A reiniciar a procura em 5 segundos...");
                try { Thread.sleep(5000); } catch (InterruptedException ignored) {}
            }
        }

        // Ciclo principal de operações para o domínio
        while (true) {
            try {
                int option = Menu();
                switch (option) {
                    case 1:
                        submeterImagem(); break;
                    case 2:
                        obterDetalhesImagem(); break;
                    case 3:
                        procurarPorCaracteristica(); break;
                    case 4:
                        escalarServidores(); break;
                    case 5:
                        escalarLabelsApp(); break;
                    case 99:
                        if (channel != null) {
                            channel.shutdown();
                        }
                        System.exit(0);
                }
            } catch (Exception ex) {
                System.out.println("Execution call Error !");
                ex.printStackTrace();
            }
        }
    }

    // Metodo que consome a Cloud Function usando o modelo HTTP do pacote java.net.http
    private static List<String> obterIps() throws Exception {
        String cfURL = "https://europe-southwest1-cn2026labels.cloudfunctions.net/lookupFunction?mig=grupo-madrid-servidores";
        HttpClient client = HttpClient.newBuilder().build();
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(cfURL))
                .GET()
                .build();

        System.out.println("A consultar a Cloud Function para obter instâncias ativas...");
        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

        if (response.statusCode() == 200) {
            Gson gson = new Gson();
            return gson.fromJson(response.body(), new TypeToken<List<String>>(){}.getType());
        } else {
            throw new RuntimeException("Código: " + response.statusCode() + " | Detalhe: " + response.body());        }
    }
    // Lê uma imagem do "computador local" e envia em blocos (stream) para o servidor gRPC
    static void submeterImagem() {
        System.out.println("--- Submeter Imagem ---");
        Scanner scanner = new Scanner(System.in);
        String filePath = read("Qual é o caminho completo da imagem (ex: C:\\imagens\\foto.jpg)?", scanner);

        Path path = Paths.get(filePath);
        if (!Files.exists(path)) {
            System.out.println("Erro: O ficheiro não existe no caminho especificado!");
            return;
        }

        CountDownLatch finishLatch = new CountDownLatch(1);

        StreamObserver<BlocoImagem> streamImagem = noBlockStub.submeterImagem(new StreamObserver<RespostaSubmissao>() {
            @Override
            public void onNext(RespostaSubmissao resposta) {
                System.out.println("-> Sucesso! A imagem foi recebida e o ID gerado é: " + resposta.getIdPedido());
            }

            @Override
            public void onError(Throwable throwable) {
                System.out.println("-> Erro na submissão: " + throwable.getMessage());
                finishLatch.countDown();
            }

            @Override
            public void onCompleted() {
                finishLatch.countDown();
            }
        });

        try {
            byte[] buffer = new byte[64 * 1024];
            String fileName = path.getFileName().toString();

            try (InputStream input = Files.newInputStream(path)) {
                int bytesLidos;
                while ((bytesLidos = input.read(buffer)) > 0) {
                    ByteString conteudo = ByteString.copyFrom(buffer, 0, bytesLidos);

                    BlocoImagem bloco = BlocoImagem.newBuilder()
                            .setNomeFicheiro(fileName)
                            .setConteudo(conteudo)
                            .build();

                    streamImagem.onNext(bloco);
                }
            }

            streamImagem.onCompleted();
            System.out.println("A enviar a imagem para o servidor, aguarde...");
            finishLatch.await();

        } catch (Exception e) {
            System.out.println("Erro ao ler ou enviar o ficheiro: " + e.getMessage());
            streamImagem.onError(e);
        }
    }

    // Consulta os detalhes da imagem (data e labels traduzidas) pelo ID da propria
    static void obterDetalhesImagem() {
        try {
            System.out.println("--- Consultar Detalhes da Imagem ---");
            String id = read("Qual é o ID da imagem?", new Scanner(System.in));

            PedidoDetalhes pedido = PedidoDetalhes.newBuilder().setIdPedido(id).build();
            RespostaDetalhes resposta = blockingStub.obterDetalhesImagem(pedido);

            System.out.println("Data de Processamento: " + resposta.getDataProcessamento());
            System.out.println("Características detetadas:");
            for (DetalheCaracteristica c : resposta.getCaracteristicasList()) {
                System.out.println(" - " + c.getNomeOriginal() + " -> " + c.getNomeTraduzido() + " (Confiança: " + c.getConfianca() + ")");
            }
        } catch(Exception ex) {
            System.out.println("Synchronous call error: " + ex.getMessage());
        }
    }

    // Pesquisa ficheiros por uma caracteristica específica dentro de um intervalo de tempo
    static void procurarPorCaracteristica() {
        try {
            System.out.println("--- Procurar Imagens por Característica ---");
            Scanner scan = new Scanner(System.in);
            String caracteristica = read("Qual a característica (ex: cat)?", scan);

            String dataInicio = "";
            String dataFim = "";

            while (true) {
                dataInicio = read("Data de Início (ex: 2026-05-01)?", scan);
                dataFim = read("Data de Fim (ex: 2026-05-30)?", scan);

                try {
                    LocalDate inicio = LocalDate.parse(dataInicio);
                    LocalDate fim = LocalDate.parse(dataFim);

                    if (fim.isBefore(inicio)) {
                        System.out.println("-> Erro: A Data de Fim não pode ser anterior à Data de Início! Tente novamente.\n");
                    } else {
                        break;
                    }
                } catch (DateTimeParseException e) {
                    System.out.println("-> Erro: Formato de data inválido! Use o formato AAAA-MM-DD (ex: 2026-05-01).\n");
                }
            }

            PedidoPesquisa pedido = PedidoPesquisa.newBuilder()
                    .setCaracteristica(caracteristica.toLowerCase().trim())
                    .setDataInicio(dataInicio)
                    .setDataFim(dataFim)
                    .build();

            RespostaPesquisa resposta = blockingStub.procurarPorCaracteristica(pedido);

            System.out.println("Ficheiros encontrados:");
            for (String nomeFicheiro : resposta.getNomesFicheirosList()) {
                System.out.println(" -> " + nomeFicheiro);
            }
        } catch(Exception ex) {
            System.out.println("Synchronous call error: " + ex.getMessage());
        }
    }

    // Envia um pedido a Google Cloud para alterar o numero de instâncias do Servidor gRPC
    static void escalarServidores() {
        try {
            System.out.println("--- Escalar Servidores gRPC ---");
            int num = Integer.parseInt(read("Quantas instâncias quer ter a correr?", new Scanner(System.in)));

            PedidoEscala pedido = PedidoEscala.newBuilder().setNumeroInstancias(num).build();
            gestaoStub.escalarServidoresGrpc(pedido);

            System.out.println("-> Pedido de escala enviado com sucesso!");
        } catch(Exception ex) {
            System.out.println("Erro na chamada: " + ex.getMessage());
        }
    }

    // Envia um pedido a Google Cloud para alterar o numero de instâncias da aplicação Labels App
    static void escalarLabelsApp() {
        try {
            System.out.println("--- Escalar Labels App (Workers) ---");
            int num = Integer.parseInt(read("Quantas instâncias quer ter a correr?", new Scanner(System.in)));

            PedidoEscala pedido = PedidoEscala.newBuilder().setNumeroInstancias(num).build();
            gestaoStub.escalarLabelsApp(pedido);

            System.out.println("-> Pedido de escala enviado com sucesso!");
        } catch(Exception ex) {
            System.out.println("Erro na chamada: " + ex.getMessage());
        }
    }

    private static int Menu() {
        int op;
        Scanner scan = new Scanner(System.in);
        do {
            System.out.println();
            System.out.println(" MENU");
            System.out.println(" 1 - Submeter nova imagem");
            System.out.println(" 2 - Consultar detalhes de uma imagem (por ID)");
            System.out.println(" 3 - Procurar imagens por característica (com datas)");
            System.out.println(" 4 - Escalar Servidores gRPC");
            System.out.println(" 5 - Escalar Labels App");
            System.out.println("99 - Exit");
            System.out.println();
            op = Integer.parseInt(read("Escolhe uma opção?", scan));
        } while (!((op >= 1 && op <= 5) || op == 99));
        return op;
    }

    private static String read(String msg, Scanner input) {
        System.out.println(msg);
        return input.nextLine();
    }
}