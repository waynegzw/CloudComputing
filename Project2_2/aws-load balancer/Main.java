import java.io.IOException;
import java.net.ServerSocket;

public class Main {
	private static final int PORT = 80;
	private static DataCenterInstance[] instances;
	private static ServerSocket serverSocket;

	//Update this list with the DNS of your data center instances
	//Remember to put your azure dns here. Don't put IP ADDRESS.
	static {
		instances = new DataCenterInstance[3];
		instances[0] = new DataCenterInstance("First", "http://ec2-54-208-7-18.compute-1.amazonaws.com");
		instances[1] = new DataCenterInstance("Second", "http://ec2-54-84-228-187.compute-1.amazonaws.com");
		instances[2] = new DataCenterInstance("Third", "http://ec2-52-91-39-64.compute-1.amazonaws.com");
	}

	public static void main(String[] args) throws IOException {
		initServerSocket();
		LoadBalancer loadBalancer = new LoadBalancer(serverSocket, instances);
		loadBalancer.start();
	}

	/**
	 * Initialize the socket on which the Load Balancer will receive requests from the Load Generator
	 */
	private static void initServerSocket() {
		try {
			serverSocket = new ServerSocket(PORT);
		} catch (IOException e) {
			System.err.println("ERROR: Could not listen on port: " + PORT);
			e.printStackTrace();
			System.exit(-1);
		}
	}
}

