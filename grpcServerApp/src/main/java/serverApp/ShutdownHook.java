package serverApp;

public class ShutdownHook extends Thread {
    io.grpc.Server svc;

    public ShutdownHook(io.grpc.Server svc) {
        this.svc = svc;
    }

    @Override
    public void run() {
        System.err.println("* A encerrar o servidor gRPC (a JVM está a desligar)");
        try {
            // Initiates an orderly shutdown in which preexisting calls continue
            // but new calls are rejected. So we can clean and finish work
            svc.shutdown();
            svc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace(System.err);
        }
        System.err.println("*** Servidor desligado com sucesso");
    }
}
