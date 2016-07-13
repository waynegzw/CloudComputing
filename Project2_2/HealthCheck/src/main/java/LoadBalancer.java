import java.io.IOException;
import java.net.ServerSocket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class LoadBalancer {
    private static final int THREAD_POOL_SIZE = 5;
    private final ServerSocket socket;
    private final DataCenterInstance[] instances;
    private static int index = 0;

    public LoadBalancer(ServerSocket socket, DataCenterInstance[] instances) {
        this.socket = socket;
        this.instances = instances;
    }

    // Complete this function
    public void start() throws IOException {
        ExecutorService executorService = Executors.newFixedThreadPool(THREAD_POOL_SIZE);
        Runnable healthCheck = new HealthCheck(instances);
        executorService.execute(healthCheck);

        while (true) {

            if (index >= 3) {
                index %= 3;
            }
            if (instances[index].getUrl() == null || instances[index].getUrl().isEmpty()) {
                index = (index + 1) % 3;
            }
            Runnable requestHandler = new RequestHandler(socket.accept(), instances[index]);
            executorService.execute(requestHandler);
            index++;
        }
    }


}

