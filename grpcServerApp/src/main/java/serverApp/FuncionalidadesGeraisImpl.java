package serverApp;

import io.grpc.stub.StreamObserver;
import pt.isel.cn2026.grpc.*;
import com.google.cloud.storage.*;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.pubsub.v1.TopicName;
import com.google.pubsub.v1.PubsubMessage;
import com.google.protobuf.ByteString;
import com.google.cloud.firestore.*;
import com.google.api.core.ApiFuture;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class FuncionalidadesGeraisImpl extends FuncionalidadesGeraisGrpc.FuncionalidadesGeraisImplBase {

    private static final String BUCKET_NAME = "cn-trabalho-final-bucket-2526"; //nome exato do bucket criado no cloud storage para armazenar todas as imagens submetidas pelo utilzador
    private static final String TOPIC_ID = "cn-image-processing"; //nome exato do tópico criado no Pub/Sub que irá recebe o id da imagem, o nome do bucket e do blob

    // Coleção guardar os resultados, demos nome para não ir "default"
    private static final String COLECAO_FIRESTORE = "imagens_processadas";

    // Clientes da Google Cloud
    private final Storage storage;
    private final Firestore db;
    private Publisher publisher;

    public FuncionalidadesGeraisImpl() {
        this.storage = StorageOptions.getDefaultInstance().getService();
        this.db = FirestoreOptions.getDefaultInstance().toBuilder()
                .setDatabaseId("cn-firestore")
                .build()
                .getService();

        // Vai buscar o ID do projeto através da chave json configurada como variavel de ambiente no SO
        String projectId = StorageOptions.getDefaultInstance().getProjectId();

        try {
            TopicName topicName = TopicName.of(projectId, TOPIC_ID);
            this.publisher = Publisher.newBuilder(topicName).build();
        } catch (IOException e) {
            System.err.println("Erro ao inicializar o Publisher do Pub/Sub: " + e.getMessage());
        }
    }

  // Recebe o stream (Bloco de Imagem) do cliente, guarda no Storage e avisa o Pub/Sub
    @Override
    public StreamObserver<BlocoImagem> submeterImagem(StreamObserver<RespostaSubmissao> responseObserver) {
        return new StreamObserver<BlocoImagem>() {
            ByteArrayOutputStream fluxoImagem = new ByteArrayOutputStream();
            String nomeFicheiroOriginal = "imagem_desconhecida";

            @Override
            public void onNext(BlocoImagem bloco) {
                try {
                    if (!bloco.getNomeFicheiro().isEmpty()) {
                        nomeFicheiroOriginal = bloco.getNomeFicheiro();
                    }
                    fluxoImagem.write(bloco.getConteudo().toByteArray());
                } catch (IOException e) {
                    System.err.println("Erro a juntar os blocos: " + e.getMessage());
                }
            }

            @Override
            public void onError(Throwable throwable) {
                System.err.println("Erro durante a receção: " + throwable.getMessage());
            }

            @Override
            public void onCompleted() {
                try {
                    String idPedido = UUID.randomUUID().toString(); // O ID é único
                    String nomeBlob = idPedido + "_" + nomeFicheiroOriginal;

                    System.out.println("[SERVIDOR] A guardar imagem no Cloud Storage...");
                    BlobId blobId = BlobId.of(BUCKET_NAME, nomeBlob);
                    BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();
                    storage.create(blobInfo, fluxoImagem.toByteArray());
                    System.out.println("[SERVIDOR] Imagem guardada com sucesso!");

                    // Prepara a mensagem para o Pub/Sub
                    String messagingTexto = idPedido + "|" + BUCKET_NAME + "|" + nomeBlob;
                    PubsubMessage mensagemPubSub = PubsubMessage.newBuilder()
                            .setData(ByteString.copyFromUtf8(messagingTexto))
                            .build();

                    System.out.println("[SERVIDOR] A publicar mensagem no Pub/Sub de forma síncrona...");
                    com.google.api.core.ApiFuture<String> future = publisher.publish(mensagemPubSub);
                    String messageId = future.get();

                    System.out.println("[SERVIDOR] Mensagem aceite pelo Pub/Sub com o ID: " + messageId);

                    // Responde ao cliente
                    RespostaSubmissao resposta = RespostaSubmissao.newBuilder()
                            .setIdPedido(idPedido)
                            .build();
                    responseObserver.onNext(resposta);
                    responseObserver.onCompleted();
                    System.out.println("[SERVIDOR] Fluxo terminado com sucesso para o ID: " + idPedido);

                } catch (Exception e) {
                    System.err.println("[SERVIDOR ERROR] Falha crítica no fluxo: " + e.getMessage());
                    e.printStackTrace(); // Para enviar o erro completo no terminal
                    responseObserver.onError(e);
                }
            }
        };
    }


    // Vai ao Firestore procurar as características pelo ID do pedido do utlizador passado na consola
    @Override
    public void obterDetalhesImagem(PedidoDetalhes request, StreamObserver<RespostaDetalhes> responseObserver) {
        String idPedido = request.getIdPedido();

        try {
            // Procura o documento pelo ID da imagem
            DocumentReference docRef = db.collection(COLECAO_FIRESTORE).document(idPedido);
            ApiFuture<DocumentSnapshot> future = docRef.get();
            DocumentSnapshot document = future.get(); // Bloqueia até ter a resposta da nuvem

            if (document.exists()) {
                RespostaDetalhes.Builder respostaBuilder = RespostaDetalhes.newBuilder();

                // Ler a data
                String dataProc = document.getString("dataProcessamento");
                if (dataProc != null) respostaBuilder.setDataProcessamento(dataProc);

                // Ler a lista de características
                List<Map<String, Object>> labelsFirestore = (List<Map<String, Object>>) document.get("caracteristicas_detalhadas");
                if (labelsFirestore != null) {
                    for (Map<String, Object> label : labelsFirestore) {
                        DetalheCaracteristica.Builder detalhe = DetalheCaracteristica.newBuilder();
                        if (label.containsKey("nomeOriginal")) detalhe.setNomeOriginal((String) label.get("nomeOriginal"));
                        if (label.containsKey("nomeTraduzido")) detalhe.setNomeTraduzido((String) label.get("nomeTraduzido"));
                        if (label.containsKey("confianca")) {
                            Object conf = label.get("confianca");
                            if (conf instanceof Double) detalhe.setConfianca(((Double) conf).floatValue());
                        }
                        respostaBuilder.addCaracteristicas(detalhe.build());
                    }
                }

                responseObserver.onNext(respostaBuilder.build());
                responseObserver.onCompleted();
            } else {
                // Se a Labels App ainda não processou a imagem... entao o documento ainda não existe
                responseObserver.onError(new io.grpc.StatusRuntimeException(
                        io.grpc.Status.NOT_FOUND.withDescription("A imagem ainda está a ser processada ou não existe.")
                ));
            }
        } catch (InterruptedException | ExecutionException e) {
            responseObserver.onError(new io.grpc.StatusRuntimeException(
                    io.grpc.Status.INTERNAL.withDescription("Erro ao aceder à base de dados: " + e.getMessage())
            ));
        }
    }

    // Filtra documentos no Firestore por datas e label
    @Override
    public void procurarPorCaracteristica(PedidoPesquisa request, StreamObserver<RespostaPesquisa> responseObserver) {
        try {
            // a Labels App vai guardar uma lista de Strings com os nomes das labels chamada "labels_simples"
            // exemplo: ["cat", "animal", "pet"]
            Query query = db.collection(COLECAO_FIRESTORE)
                    .whereArrayContains("labels_simples", request.getCaracteristica())
                    .whereGreaterThanOrEqualTo("dataProcessamento", request.getDataInicio())
                    .whereLessThanOrEqualTo("dataProcessamento", request.getDataFim());

            ApiFuture<QuerySnapshot> querySnapshot = query.get();
            List<QueryDocumentSnapshot> documents = querySnapshot.get().getDocuments();

            RespostaPesquisa.Builder respostaBuilder = RespostaPesquisa.newBuilder();
            for (DocumentSnapshot document : documents) {
                String nomeFicheiro = document.getString("nomeBlob");
                if (nomeFicheiro != null) {
                    respostaBuilder.addNomesFicheiros(nomeFicheiro);
                }
            }

            responseObserver.onNext(respostaBuilder.build());
            responseObserver.onCompleted();

        } catch (InterruptedException | ExecutionException e) {
            responseObserver.onError(new io.grpc.StatusRuntimeException(
                    io.grpc.Status.INTERNAL.withDescription("Erro na pesquisa: " + e.getMessage())
            ));
        }
    }
}