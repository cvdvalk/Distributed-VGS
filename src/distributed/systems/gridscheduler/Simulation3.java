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
public class Simulation3 implements Runnable {

	/**
	 * Application entry point.
	 * @param args application parameters
	 * @throws NotBoundException 
	 * @throws RemoteException 
	 * @throws AlreadyBoundException 
	 * @throws InterruptedException 
	 */
	public static void main(String[] args) throws RemoteException, NotBoundException, AlreadyBoundException, InterruptedException {
		
		
		GridSchedulerNode node1 = new GridSchedulerNode("Node1", "192.168.1.126", 1099);
//		GridSchedulerNode node2 = new GridSchedulerNode("Node2", "localhost", 1100);
		
		node1.connectToGridScheduler("Node2", "192.168.1.106", 1100);

		Cluster cluster1 = new Cluster("cluster1", "Node1", 32, "192.168.1.126", 1104, "192.168.1.126", 1099);
//		Cluster cluster2 = new Cluster("cluster2", "Node1", 32, "192.168.1.126", 1105, "192.168.1.126", 1099);
//		Cluster cluster3 = new Cluster("cluster3", "Node1", 32,"192.168.1.126", 1106, "192.168.1.126", 1099);
//		Cluster cluster4 = new Cluster("cluster4", "Node1", 32,"192.168.1.126", 1107, "192.168.1.126", 1099);//128
		
//		Cluster cluster5 = new Cluster("cluster5", "Node2", 64, "localhost", 1108, "localhost", 1100);
//		Cluster cluster6 = new Cluster("cluster6", "Node2", 64, "localhost", 1109, "localhost", 1100);
//		Cluster cluster7 = new Cluster("cluster7", "Node2", 64, "localhost", 1110, "localhost", 1100);
//		Cluster cluster8 = new Cluster("cluster8", "Node2", 64, "localhost", 1111, "localhost", 1100);//256
		
		int xtrajobs = 0;
		int jobsNumber = 100;
		for(int i = 0;i < jobsNumber;i++){
			Job job = new Job(8000 + (int)(Math.random() * 5000), i);
			cluster1.getResourceManager().addJob(job);

			try {
				// Sleep a while before creating a new job
				Thread.sleep(100L);
			} catch (InterruptedException e) {
				assert(false) : "Simulation runtread was interrupted";
			}
		}
		
		
	}

	@Override
	public void run() {
		// TODO Auto-generated method stub
		
	}

}

