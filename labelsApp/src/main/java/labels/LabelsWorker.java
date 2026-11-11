package labels;

import com.google.cloud.firestore.Firestore;
import com.google.cloud.firestore.FirestoreOptions;
import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.cloud.translate.Translate;
import com.google.cloud.translate.TranslateOptions;
import com.google.cloud.translate.Translation;
import com.google.cloud.vision.v1.*;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.PubsubMessage;

import java.text.SimpleDateFormat;
import java.util.*;

public class LabelsWorker {

    private static final String SUBSCRIPTION_ID = "cn-labels-App"; //nome da subscrição criada no projeto na google cloud
    private static final String COLECAO_FIRESTORE = "imagens_processadas"; //nome da coleção que será criada no firestore quando este receber as informações necessárias das imagens(id, caracteristas, tradução, etc...)

    public static void main(String[] args) {
        //Inicializar os clientes da Google Cloud automaticamente
        String projectId = FirestoreOptions.getDefaultInstance().getProjectId(); //vai buscar o id do projeto através da variavel de ambiente definida no SO com a chave e, formato json da service account criada
        Firestore db = FirestoreOptions.getDefaultInstance().toBuilder()
                .setDatabaseId("cn-firestore") // Mantém a ligação à base de dados criada no Firestore
                .build()
                .getService();
        Translate translateService = TranslateOptions.getDefaultInstance().getService();

        // Definir o que acontece quando chega uma mensagem
        MessageReceiver receiver = new MessageReceiver() {
            @Override
            public void receiveMessage(PubsubMessage message, AckReplyConsumer consumer) {
                try {
                    // A mensagem vem no formato: idPedido|BUCKET_NAME|nomeBlob
                    String mensagemTexto = message.getData().toStringUtf8();
                    System.out.println("Nova mensagem recebida: " + mensagemTexto);

                    String[] partes = mensagemTexto.split("\\|");
                    if (partes.length == 3) {
                        String idPedido = partes[0];
                        String bucketName = partes[1];
                        String blobName = partes[2];

                        // Processar a imagem e guardar na base de dados as informações necessárias
                        processarImagem(idPedido, bucketName, blobName, db, translateService);
                    }

                    // Confirmar ao Pub/Sub que o trabalho foi feito com sucesso (Ack)
                    consumer.ack();

                } catch (Exception e) {
                    System.err.println("Erro ao processar a mensagem: " + e.getMessage());
                    // Se houver um erro grave, dizemos ao Pub/Sub que falhámos (Nack),
                    // para ele tentar entregar a mensagem novamente mais tarde
                    consumer.nack();
                }
            }
        };

        // Ligar o trabalhador (Subscriber) e mantê-lo a correr por tempo indeterminado
        Subscriber subscriber = null;
        try {
            ProjectSubscriptionName subName = ProjectSubscriptionName.of(projectId, SUBSCRIPTION_ID);
            subscriber = Subscriber.newBuilder(subName, receiver).build();
            subscriber.startAsync().awaitRunning();
            System.out.println("Labels App a escutar mensagens na subscrição: " + SUBSCRIPTION_ID);

            // Mantém a aplicação a correr até a fecharmos manualmente
            subscriber.awaitTerminated();
        } catch (Exception e) {
            System.err.println("Erro crítico no Subscriber: " + e.getMessage());
        } finally {
            if (subscriber != null) {
                subscriber.stopAsync();
            }
        }
    }

    // Lógica de processamento
    private static void processarImagem(String idPedido, String bucketName, String blobName, Firestore db, Translate translateService) throws Exception {
        System.out.println("A analisar a imagem: " + blobName);

        String gsUri = "gs://" + bucketName + "/" + blobName;
        List<String> labelsSimples = new ArrayList<>();
        List<Map<String, Object>> caracteristicasDetalhadas = new ArrayList<>();

        // Preparar a chamada à Vision API
        Image img = Image.newBuilder().setSource(ImageSource.newBuilder().setImageUri(gsUri).build()).build();
        Feature feat = Feature.newBuilder().setType(Feature.Type.LABEL_DETECTION).build();
        AnnotateImageRequest request = AnnotateImageRequest.newBuilder().addFeatures(feat).setImage(img).build();

        try (ImageAnnotatorClient client = ImageAnnotatorClient.create()) {
            BatchAnnotateImagesResponse response = client.batchAnnotateImages(Collections.singletonList(request));

            for (AnnotateImageResponse res : response.getResponsesList()) {
                if (res.hasError()) {
                    throw new RuntimeException("Erro reportado pela Vision API: " + res.getError().getMessage());
                }

                // Extrair as caracteristicas, nivel de confiança e realizar a tradução
                for (EntityAnnotation annotation : res.getLabelAnnotationsList()) {
                    String termoIngles = annotation.getDescription();
                    float confianca = annotation.getScore();

                    // Traduzir para Português
                    Translation translation = translateService.translate(
                            termoIngles,
                            Translate.TranslateOption.sourceLanguage("en"),
                            Translate.TranslateOption.targetLanguage("pt")
                    );
                    String termoPortugues = translation.getTranslatedText();

                    // Guardar nas listas
                    labelsSimples.add(termoIngles);

                    Map<String, Object> detalhe = new HashMap<>();
                    detalhe.put("nomeOriginal", termoIngles);
                    detalhe.put("nomeTraduzido", termoPortugues);
                    detalhe.put("confianca", (double) confianca);
                    caracteristicasDetalhadas.add(detalhe);

                    System.out.println(" -> Detetado: " + termoIngles + " / " + termoPortugues + " (" + confianca + ")");
                }
            }
        }

        // Guardar tudo no Firestore
        guardarNoFirestore(idPedido, blobName, labelsSimples, caracteristicasDetalhadas, db);
    }

    private static void guardarNoFirestore(String idPedido, String blobName, List<String> labelsSimples, List<Map<String, Object>> caracteristicasDetalhadas, Firestore db) throws Exception {
        String dataHoje = new SimpleDateFormat("yyyy-MM-dd").format(new Date());

        Map<String, Object> documento = new HashMap<>();
        documento.put("dataProcessamento", dataHoje);
        documento.put("nomeBlob", blobName);
        documento.put("labels_simples", labelsSimples);
        documento.put("caracteristicas_detalhadas", caracteristicasDetalhadas);

        // Escreve os dados no banco de dados "cn-firestore", na gaveta "imagens_processadas"
        db.collection(COLECAO_FIRESTORE).document(idPedido).set(documento).get();

        System.out.println("Relatório guardado no Firestore com sucesso para o ID: " + idPedido);
        System.out.println("--------------------------------------------------");
    }
}