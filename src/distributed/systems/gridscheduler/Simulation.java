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
		 // Bind the remote object's stub in the registry
	    Registry registry = LocateRegistry.getRegistry();
	    
		registry.bind("Node1", node1);
		registry.bind("Node2", node2);
		//Thread t = new Thread(node1);
		
		node1.connectToGridScheduler("Node2");
		
		Cluster cluster1 = new Cluster("cluster1", "Node1", 32);
		Cluster cluster4 = new Cluster("cluster4", "Node1", 64);
		Cluster cluster2 = new Cluster("cluster2", "Node2", 32);
		Cluster cluster3 = new Cluster("cluster3", "Node2", 32);
		int xtrajobs = 0;
		for(int i = 0;i < 300;i++){
			Job job = new Job(8000 + (int)(Math.random() * 5000), i);
			cluster1.getResourceManager().addJob(job);
			if(i % 2 == 0){
				xtrajobs++;
				Job job2 = new Job(8000 + (int)(Math.random() * 5000), 300+xtrajobs);
				cluster3.getResourceManager().addJob(job2);
				
			}
			if(i % 3 == 0){
				xtrajobs++;
				Job job3 = new Job(8000 + (int)(Math.random() * 5000), 300+xtrajobs);
				cluster2.getResourceManager().addJob(job3);
				
			}
			if(i % 4 == 0){
				xtrajobs++;
				Job job4 = new Job(8000 + (int)(Math.random() * 5000), 300+xtrajobs);
				cluster4.getResourceManager().addJob(job4);
				
			}
			try {
				// Sleep a while before creating a new job
				Thread.sleep(100L);
			} catch (InterruptedException e) {
				assert(false) : "Simulation runtread was interrupted";
			}
		}
		System.out.println(xtrajobs);
		//t.start();
		
	}

	@Override
	public void run() {
		// TODO Auto-generated method stub
		
	}

}
