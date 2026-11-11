package serverApp;

import io.grpc.ServerBuilder;
import java.io.IOException;

public class GrpcServer {

    private static int svcPort = 8000;

    static void main(String[] args) throws InterruptedException, IOException {
        if (args.length > 0) svcPort = Integer.parseInt(args[0]);
        io.grpc.Server svc = ServerBuilder.forPort(svcPort)
                // Add one or more services.
                // The Server can host many services in same TCP/IP port
                .addService(new FuncionalidadesGeraisImpl())
                .addService(new GestaoElasticidadeImpl())
                .build();
        svc.start();
        System.out.println("Server started on port " + svcPort);
        // Java virtual machine shutdown hook
        // to capture normal or abnormal exits
        Runtime.getRuntime().addShutdownHook(new ShutdownHook(svc));
        // Waits for the server to become terminated
        svc.awaitTermination();
    }
}
