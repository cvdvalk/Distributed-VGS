package distributed.systems.gridscheduler;

import java.rmi.AlreadyBoundException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.ArrayList;
import java.util.List;

import javax.swing.JFrame;

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
public class Simulation implements Runnable {

	/**
	 * Application entry point.
	 * @param args application parameters
	 * @throws NotBoundException 
	 * @throws RemoteException 
	 * @throws AlreadyBoundException 
	 * @throws InterruptedException 
	 */
	public static void main(String[] args) throws RemoteException, NotBoundException, AlreadyBoundException, InterruptedException {
		// Create and run the simulation
//		try {
//			java.rmi.registry.LocateRegistry.createRegistry(1099);
//		} catch (RemoteException e) {
//			e.printStackTrace();
//		}
		
		List<Cluster> clusters = new ArrayList<Cluster>();
		
		GridSchedulerNode node1 = new GridSchedulerNode("Node1", "localhost", 1099);
		GridSchedulerNode node2 = new GridSchedulerNode("Node2", "localhost", 1100);
		GridSchedulerNode node3 = new GridSchedulerNode("Node3", "localhost", 1101);
		GridSchedulerNode node4 = new GridSchedulerNode("Node4", "localhost", 1102);
		GridSchedulerNode node5 = new GridSchedulerNode("Node5", "localhost", 1103);
		 // Bind the remote object's stub in the registry
//	    Registry registry = LocateRegistry.getRegistry();
//	    
//		registry.bind("Node1", node1);
//		registry.bind("Node2", node2);
//		registry.bind("Node3", node3);
//		registry.bind("Node4", node4);
//		registry.bind("Node5", node5);
		
		node1.connectToGridScheduler("Node2", "localhost", 1100);
		node2.connectToGridScheduler("Node3", "localhost", 1101);
		node3.connectToGridScheduler("Node4", "localhost", 1102);
		node4.connectToGridScheduler("Node5", "localhost", 1103);
		node5.connectToGridScheduler("Node1", "localhost", 1099);
		
		node1.connectToGridScheduler("Node3", "localhost", 1101);
		node1.connectToGridScheduler("Node4", "localhost", 1102);
		node2.connectToGridScheduler("Node4", "localhost", 1102);
		node2.connectToGridScheduler("Node5", "localhost", 1103);
		node3.connectToGridScheduler("Node5", "localhost", 1103);
		
		Cluster cluster1 = new Cluster("cluster1", "Node1", 32, "localhost", 1104, "localhost", 1099);
		Cluster cluster2 = new Cluster("cluster2", "Node1", 32, "localhost", 1105, "localhost", 1099);
//		Cluster cluster3 = new Cluster("cluster3", "Node1", 32);
//		Cluster cluster4 = new Cluster("cluster4", "Node1", 32);//128
		clusters.add(cluster1);clusters.add(cluster2);
		
		Cluster cluster5 = new Cluster("cluster5", "Node2", 32, "localhost", 1106, "localhost", 1100);
		Cluster cluster6 = new Cluster("cluster6", "Node2", 32, "localhost", 1107, "localhost", 1100);
//		Cluster cluster7 = new Cluster("cluster7", "Node2", 32);
//		Cluster cluster8 = new Cluster("cluster8", "Node2", 32);//192
		clusters.add(cluster5);clusters.add(cluster6);
		
		Cluster cluster9 = new Cluster("cluster9", "Node3", 32, "localhost", 1108, "localhost", 1101);
		Cluster cluster10 = new Cluster("cluster10", "Node3", 32, "localhost", 1109, "localhost", 1101);
//		Cluster cluster11 = new Cluster("cluster11", "Node3", 64);
//		Cluster cluster12 = new Cluster("cluster12", "Node3", 32);//192
		clusters.add(cluster9);clusters.add(cluster10);
		
		Cluster cluster13 = new Cluster("cluster13", "Node4", 32, "localhost", 1110, "localhost", 1102);
		Cluster cluster14 = new Cluster("cluster14", "Node4", 32, "localhost", 1111, "localhost", 1102);
//		Cluster cluster15 = new Cluster("cluster15", "Node4", 64);
//		Cluster cluster16 = new Cluster("cluster16", "Node4", 128);//288
		clusters.add(cluster13);clusters.add(cluster14);

		Cluster cluster17 = new Cluster("cluster17", "Node5", 32, "localhost", 1112, "localhost", 1103);
		Cluster cluster18 = new Cluster("cluster18", "Node5", 32, "localhost", 1113, "localhost", 1103);
		Cluster cluster19 = new Cluster("cluster19", "Node5", 64, "localhost", 1114, "localhost", 1103);
//		Cluster cluster20 = new Cluster("cluster20", "Node5", 128);//256
		clusters.add(cluster17);clusters.add(cluster18);clusters.add(cluster19);
		
		int xtrajobs = 0;
		int jobsNumber = 500;
		for(int i = 0;i < jobsNumber;i++){
			Job job = new Job(8000 + (int)(Math.random() * 5000), i);
			cluster1.getResourceManager().addJob(job);

			if(i % 2 == 0){
				xtrajobs++;
				Job job2 = new Job(8000 + (int)(Math.random() * 5000), jobsNumber+xtrajobs);
				cluster5.getResourceManager().addJob(job2);
				
				
			}
			xtrajobs++;
			Job job3 = new Job(8000 + (int)(Math.random() * 5000), jobsNumber+xtrajobs);
			cluster5.getResourceManager().addJob(job3);
			if(i % 5 == 0){
				for(Cluster c : clusters){
					xtrajobs++;
					Job job_temp = new Job(8000 + (int)(Math.random() * 5000), jobsNumber+xtrajobs);
					c.getResourceManager().addJob(job_temp);
				}
			}
			if(i==100){
				System.out.println("Node3 crash");
				node3.NodeCrash();
			}
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
