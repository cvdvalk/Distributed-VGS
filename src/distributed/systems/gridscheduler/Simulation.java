package distributed.systems.gridscheduler;

import java.rmi.AlreadyBoundException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

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
 * @author Niels Brouwers, Boaz Pat-El
 */
public class Simulation implements Runnable {
	// Number of clusters in the simulation
	private final static int nrClusters = 5;

	// Number of nodes per cluster in the simulation
	private final static int nrNodes = 50;
	
	// Simulation components
	Cluster clusters[];
	
//	GridSchedulerPanel gridSchedulerPanel;
//	
//	/**
//	 * Constructs a new simulation object. Study this code to see how to set up your own
//	 * simulation.
//	 */
//	public Simulation() {
//		GridScheduler scheduler;
//		
//		// Setup the model. Create a grid scheduler and a set of clusters.
//		scheduler = new GridScheduler("scheduler1");
//
//		// Create a new gridscheduler panel so we can monitor our components
//		gridSchedulerPanel = new GridSchedulerPanel(scheduler);
//		gridSchedulerPanel.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
//
//		// Create the clusters and nods
//		clusters = new Cluster[nrClusters];
//		for (int i = 0; i < nrClusters; i++) {
//			clusters[i] = new Cluster("cluster" + i, scheduler.getUrl(), nrNodes); 
//			
//			// Now create a cluster status panel for each cluster inside this gridscheduler
//			ClusterStatusPanel clusterReporter = new ClusterStatusPanel(clusters[i]);
//			gridSchedulerPanel.addStatusPanel(clusterReporter);
//		}
//		
//		// Open the gridscheduler panel
//		gridSchedulerPanel.start();
//		
//		// Run the simulation
//		Thread runThread = new Thread(this);
//		runThread.run(); // This method only returns after the simulation has ended
//		
//		// Now perform the cleanup
//		
//		// Stop clusters
//		for (Cluster cluster : clusters)
//			cluster.stopPollThread();
//		
//		// Stop grid scheduler
//		scheduler.stopPollThread();
//	}
//
//	/**
//	 * The main run thread of the simulation. You can tweak or change this code to produce
//	 * different simulation scenarios. 
//	 */
//	public void run() {
//		long jobId = 0;
//		// Do not stop the simulation as long as the gridscheduler panel remains open
//		while (gridSchedulerPanel.isVisible()) {
//			// Add a new job to the system that take up random time
//			Job job = new Job(8000 + (int)(Math.random() * 5000), jobId++);
//			clusters[0].getResourceManager().addJob(job);
//			
//			try {
//				// Sleep a while before creating a new job
//				Thread.sleep(100L);
//			} catch (InterruptedException e) {
//				assert(false) : "Simulation runtread was interrupted";
//			}
//			
//		}
//
//	}

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
//		new Simulation();
		try {
			java.rmi.registry.LocateRegistry.createRegistry(1099);
		} catch (RemoteException e) {
			e.printStackTrace();
		}
		GridSchedulerNode node1 = new GridSchedulerNode("Node1");
		GridSchedulerNode node2 = new GridSchedulerNode("Node2");
		GridSchedulerNode node3 = new GridSchedulerNode("Node3");
		GridSchedulerNode node4 = new GridSchedulerNode("Node4");
		GridSchedulerNode node5 = new GridSchedulerNode("Node5");
		 // Bind the remote object's stub in the registry
	    Registry registry = LocateRegistry.getRegistry();
	    
		registry.bind("Node1", node1);
		registry.bind("Node2", node2);
		registry.bind("Node3", node3);
		registry.bind("Node4", node4);
		registry.bind("Node5", node5);
		
		node1.connectToGridScheduler("Node2");
		node2.connectToGridScheduler("Node3");
//		node3.connectToGridScheduler("Node4");
//		node4.connectToGridScheduler("Node5");
//		node5.connectToGridScheduler("Node1");
		node1.connectToGridScheduler("Node3");
		
		Cluster cluster1 = new Cluster("cluster1", "Node1", 32);
//		Cluster cluster2 = new Cluster("cluster2", "Node1", 32);
//		Cluster cluster3 = new Cluster("cluster3", "Node1", 32);
//		Cluster cluster4 = new Cluster("cluster4", "Node1", 32);//128
		
		Cluster cluster5 = new Cluster("cluster5", "Node2", 64);
		Cluster cluster6 = new Cluster("cluster6", "Node2", 64);
		Cluster cluster7 = new Cluster("cluster7", "Node2", 32);
		Cluster cluster8 = new Cluster("cluster8", "Node2", 32);//192
		
		Cluster cluster9 = new Cluster("cluster9", "Node3", 32);
		Cluster cluster10 = new Cluster("cluster10", "Node3", 64);
		Cluster cluster11 = new Cluster("cluster11", "Node3", 64);
		Cluster cluster12 = new Cluster("cluster12", "Node3", 32);//192
		
//		Cluster cluster13 = new Cluster("cluster13", "Node4", 32);
//		Cluster cluster14 = new Cluster("cluster14", "Node4", 64);
//		Cluster cluster15 = new Cluster("cluster15", "Node4", 64);
//		Cluster cluster16 = new Cluster("cluster16", "Node4", 128);//288
//		
//		Cluster cluster17 = new Cluster("cluster17", "Node5", 32);
//		Cluster cluster18 = new Cluster("cluster18", "Node5", 32);
//		Cluster cluster19 = new Cluster("cluster19", "Node5", 64);
//		Cluster cluster20 = new Cluster("cluster20", "Node5", 128);//256
		
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
//			if(i % 3 == 0){
//				xtrajobs++;
//				Job job3 = new Job(8000 + (int)(Math.random() * 5000), jobsNumber+xtrajobs);
//				cluster9.getResourceManager().addJob(job3);
//				
//			}
//			if(i % 4 == 0){
//				xtrajobs++;
//				Job job4 = new Job(8000 + (int)(Math.random() * 5000), jobsNumber+xtrajobs);
//				cluster4.getResourceManager().addJob(job4);
//				
//			}
//			if(i % 5 == 0){
//				xtrajobs++;
//				Job job5 = new Job(8000 + (int)(Math.random() * 5000), jobsNumber+xtrajobs);
//				cluster17.getResourceManager().addJob(job5);
//				
//			}
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
