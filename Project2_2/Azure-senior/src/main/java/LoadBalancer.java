import java.io.IOException;
import java.net.ServerSocket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class LoadBalancer {
    private static final int THREAD_POOL_SIZE = 5;
    private final ServerSocket socket;
    private final DataCenterInstance[] instances;
    private double[] cpus = new double[3];
    private static int n = 0;

    public LoadBalancer(ServerSocket socket, DataCenterInstance[] instances) {
        this.socket = socket;
        this.instances = instances;
    }

    // Complete this function
    public void start() throws IOException {
        ExecutorService executorService = Executors.newFixedThreadPool(THREAD_POOL_SIZE);
        //create a thread to do health check
        Runnable healthCheck = new HealthCheck(instances);
        executorService.execute(healthCheck);
        //create a thread to get cpu
        Runnable getCpu = new GetCpu(instances, cpus);
        executorService.execute(getCpu);
        int cnt = 0;

        while (true) {
            int index = 0;
            //if any one of the instances is down, use round robin send request to the rest two instances
            if (instances[0].getUrl().isEmpty() || instances[1].getUrl().isEmpty() || instances[2].getUrl().isEmpty()) {
                while (instances[n].getUrl().isEmpty()) {
                    n = (n + 1) % 3;
                }
                Runnable requestHandler = new RequestHandler(socket.accept(), instances[n]);
                executorService.execute(requestHandler);
                n = (n + 1) % 3;
            } else {
                //when all the instances are available
                if (cnt >= 5) {
                    cnt %= 5;
                }
                //group five requests, send first three requests using round robin
                //the rest two requests, send to the lowest cpu
                if (cnt < 3) {
                    index = cnt;
                } else {
                    //find the instance with minimum cpu
                    for (int i = 1; i < 3; i++) {
                        if (cpus[i] < cpus[index]) {
                            index = i;
                        }
                    }
                    //find the instance with middle cpu
                    if (cnt == 4) {
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

                Runnable requestHandler = new RequestHandler(socket.accept(), instances[index]);
                executorService.execute(requestHandler);
                cnt++;
            }
        }
    }


}

