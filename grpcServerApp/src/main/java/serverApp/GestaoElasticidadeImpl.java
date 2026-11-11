package serverApp;

import com.google.cloud.storage.StorageOptions;
import io.grpc.stub.StreamObserver;
import pt.isel.cn2026.grpc.*;

import com.google.cloud.compute.v1.InstanceGroupManagersClient;
import com.google.cloud.compute.v1.ResizeInstanceGroupManagerRequest;

public class GestaoElasticidadeImpl extends GestaoElasticidadeGrpc.GestaoElasticidadeImplBase {

    private static final String PROJECT_ID = StorageOptions.getDefaultInstance().getProjectId();
    private static final String ZONE = "europe-southwest1-a"; //madrid

    //nome exato do grupo de instancias criado no projeto na google cloud
    private static final String MIG_SERVIDORES = "grupo-madrid-servidores";
    private static final String MIG_LABELS_APP = "grupo-madrid-workers";

    // Altera o número de VMs ativas no grupo de instâncias dos Servidores gRPC
    @Override
    public void escalarServidoresGrpc(PedidoEscala request, StreamObserver<RespostaEscala> responseObserver) {
        System.out.println("[ELASTICIDADE] Pedido para escalar Servidores gRPC para: " + request.getNumeroInstancias());

        try {
            redimensionarMIG(MIG_SERVIDORES, request.getNumeroInstancias());

            RespostaEscala resposta = RespostaEscala.newBuilder()
                    .setSucesso(true)
                    .setMensagem("Servidores gRPC redimensionados com sucesso.")
                    .build();
            responseObserver.onNext(resposta);
            responseObserver.onCompleted();
        } catch (Exception e) {
            System.err.println("Erro ao escalar servidores: " + e.getMessage());
            responseObserver.onError(e);
        }
    }

    @Override
    public void escalarLabelsApp(PedidoEscala request, StreamObserver<RespostaEscala> responseObserver) {
        System.out.println("[ELASTICIDADE] Pedido para escalar Labels App para: " + request.getNumeroInstancias());

        try {
            redimensionarMIG(MIG_LABELS_APP, request.getNumeroInstancias());

            RespostaEscala resposta = RespostaEscala.newBuilder()
                    .setSucesso(true)
                    .setMensagem("Labels App redimensionada com sucesso.")
                    .build();
            responseObserver.onNext(resposta);
            responseObserver.onCompleted();
        } catch (Exception e) {
            System.err.println("Erro ao escalar Labels App: " + e.getMessage());
            responseObserver.onError(e);
        }
    }

    // Comunicar com a Google Cloud para gerir a elasticidade
    private void redimensionarMIG(String nomeMIG, int novoTamanho) throws Exception {
        try (InstanceGroupManagersClient client = InstanceGroupManagersClient.create()) {

            ResizeInstanceGroupManagerRequest request = ResizeInstanceGroupManagerRequest.newBuilder()
                    .setProject(PROJECT_ID)
                    .setZone(ZONE)
                    .setInstanceGroupManager(nomeMIG)
                    .setSize(novoTamanho)
                    .build();

            // Executa o pedido e aguarda que seja recebido pela Google Cloud
            client.resizeAsync(request).get();
            System.out.println("[ELASTICIDADE] Ordem enviada à Google Cloud com sucesso para o MIG: " + nomeMIG);
        }
    }
}
