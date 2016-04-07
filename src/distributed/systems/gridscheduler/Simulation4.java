package distributed.systems.gridscheduler;

import java.rmi.AlreadyBoundException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.List;

import distributed.systems.gridscheduler.model.Cluster;
import distributed.systems.gridscheduler.model.GridSchedulerNode;
import distributed.systems.gridscheduler.model.Job;

/**
 *
 * The Simulation class is an example of a grid computation scenario. Every 100 milliseconds 
 * a new job is added to first cluster. As this cluster is swarmed with jobs, it offloads
 * some of them to the grid scheduler, wich in turn passes them to the other clusters.
 * 
 * @author Niels Brouwers, Boaz Pat-El edited by Carlo van der Valk
 */
public class Simulation4 implements Runnable {

	/**
	 * Application entry point.
	 * @param args application parameters
	 * @throws NotBoundException 
	 * @throws RemoteException 
	 * @throws AlreadyBoundException 
	 * @throws InterruptedException 
	 */
	public static void main(String[] args) throws RemoteException, NotBoundException, AlreadyBoundException, InterruptedException {
		
		List<Cluster> clusters = new ArrayList<Cluster>();
		
//		GridSchedulerNode node1 = new GridSchedulerNode("Node1", "192.168.1.106", 1099);
		GridSchedulerNode node2 = new GridSchedulerNode("Node2", "192.168.1.106", 1100);
		
//		node1.connectToGridScheduler("Node2", "192.168.1.106", 1100);


		
		Cluster cluster5 = new Cluster("cluster5", "Node2", 64, "192.168.1.106", 1108, "192.168.1.106", 1100);
		Cluster cluster6 = new Cluster("cluster6", "Node2", 64, "192.168.1.106", 1109, "192.168.1.106", 1100);
		Cluster cluster7 = new Cluster("cluster7", "Node2", 64, "192.168.1.106", 1110, "192.168.1.106", 1100);
		Cluster cluster8 = new Cluster("cluster8", "Node2", 64, "192.168.1.106", 1111, "192.168.1.106", 1100);//256
		

		
		
	}

	@Override
	public void run() {
		// TODO Auto-generated method stub
		
	}

}

