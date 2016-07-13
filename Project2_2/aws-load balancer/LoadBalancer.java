import java.io.IOException;
import java.net.ServerSocket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class LoadBalancer {
    private static final int THREAD_POOL_SIZE = 5;
    private final ServerSocket socket;
    private final DataCenterInstance[] instances;
    private double[] cpus = new double[3];

    public LoadBalancer(ServerSocket socket, DataCenterInstance[] instances) {
        this.socket = socket;
        this.instances = instances;
    }

    // Complete this function
    public void start() throws IOException {
        ExecutorService executorService = Executors.newFixedThreadPool(THREAD_POOL_SIZE);
        Runnable getCpu = new GetCpu(instances, cpus);
        executorService.execute(getCpu);

        int cnt = 0;

        while (true) {

            int index = 0;

            if (cnt >= 6) {
                cnt %= 6;
            }

            if (cnt < 3) {
                index = cnt;
            } else {
                for (int i = 1; i < 3; i++) {
                    if (cpus[i] < cpus[index]) {
                        index = i;
                    }
                }
                if (cnt == 5) {
                    if (index == 0) {
                        if (cpus[1] < cpus[2]) {
                            index = 1;
                        } else {
                            index = 2;
                        }
                    } else if (index == 1) {
                        if (cpus[0] < cpus[2]) {
                            index = 0;
                        } else {
                            index = 2;
                        }
                    } else {
                        if (cpus[0] < cpus[1]) {
                            index = 0;
                        } else {
                            index = 1;
                        }
                    }
                }
            }

            // By default, it will send all requests to the first instance
            Runnable requestHandler = new RequestHandler(socket.accept(), instances[index]);
            executorService.execute(requestHandler);
            cnt++;
        }
    }


}


