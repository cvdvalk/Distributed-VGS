package distributed.systems.gridscheduler.model;

import java.rmi.AlreadyBoundException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.ArrayList;
import java.util.List;

/**
 * 
 * The Cluster class represents a single cluster in the virtual grid system. It consists of a 
 * collection of nodes and a resource manager. 
 * 
 * @author Niels Brouwers edited by Carlo van der Valk
 *
 */
public class Cluster implements Runnable {
	private List <Node> nodes;
	private ResourceManager resourceManager;
	private String url;
	
	// polling frequency, 10hz
	private long pollSleep = 100;
	
	// polling thread
	private Thread pollingThread;
	private boolean running;
	private int nodeCount;
	
	/**
	 * Creates a new Cluster, with a number of nodes and a resource manager
	 * <p>
	 * <DL>
	 * <DT><B>Preconditions:</B> 
	 * <DD>parameter <CODE>name</CODE> cannot be null<br>
	 * <DD>parameter <CODE>gridSchedulerURL</CODE> cannot be null<br>
	 * <DD>parameter <CODE>nrNodes</code> must be greater than 0
	 * </DL>
	 * @param name the name of this cluster
	 * @param nrNodes the number of nodes in this cluster
	 * @throws RemoteException 
	 * @throws NotBoundException 
	 * @throws AlreadyBoundException 
	 * @throws InterruptedException 
	 */
	public Cluster(String name, String gridSchedulerURL, int nodeCount, String adress, int port, String nodeAdress, int nodePort) throws RemoteException, NotBoundException, AlreadyBoundException, InterruptedException {
		// Preconditions
		assert(name != null) : "parameter 'name' cannot be null";
		assert(gridSchedulerURL != null) : "parameter 'gridSchedulerURL' cannot be null";
		assert(nodeCount > 0) : "parameter 'nodeCount' cannot be smaller or equal to zero";
		
		// Initialize members
		this.url = name;
		this.nodeCount = nodeCount;

		System.out.println("Creating cluster: " + url);
		nodes = new ArrayList<Node>(nodeCount);
		
		// Initialize the resource manager for this cluster
		resourceManager = new ResourceManager(this, adress, port);
		
		try {
			System.setProperty("java.rmi.server.hostname", adress); 
			java.rmi.registry.LocateRegistry.createRegistry(port);
		} catch (RemoteException e) {
			e.printStackTrace();
		}
		
		// Bind the remote object's stub in the registry
	    Registry registry = LocateRegistry.getRegistry(port);
	    
		registry.bind(url, resourceManager);
		
		resourceManager.connectToGridScheduler(gridSchedulerURL, nodeAdress, nodePort);

		// Initialize the nodes 
		for (int i = 0; i < nodeCount; i++) {
			Node n = new Node();
			
			// Make nodes report their status to the resource manager
			n.addNodeEventHandler(resourceManager);
			nodes.add(n);
		}
		
		// Start the polling thread
		running = true;
		pollingThread = new Thread(this);
		pollingThread.start();
		
	}

	/**
	 * Returns the number of nodes in this cluster. 
	 * @return the number of nodes in this cluster
	 */
	public int getNodeCount() {
		return nodeCount;
		//return nodes.size();
	}

	/**
	 * Returns the resource manager object for this cluster.
	 * @return the resource manager object for this cluster
	 */
	public ResourceManager getResourceManager() {
		return resourceManager;
	}

	/**
	 * Returns the url of the cluster
	 * @return the url of the cluster
	 */
	public String getName() {
		return url;
	}

	/**
	 * Returns the nodes inside the cluster as an array.
	 * @return an array of Node objects
	 */
	public List<Node> getNodes() {
		return nodes;
	}
	
	/**
	 * Finds a free node and returns it. If no free node can be found, the method returns null.
	 * @return a free Node object, or null if no such node can be found. 
	 */
	public Node getFreeNode() {
		// Find a free node among the nodes in our cluster
	    for (Node node : nodes){
			if (node.getStatus() == NodeStatus.Idle) {
				return node;
			}
	    }
		// if we haven't returned from the function here, we haven't found a suitable node
		// so we just return null
		return null;
	}

	/**
	 * Polling thread runner. This function polls each node in the system repeatedly. Polling
	 * is needed to make each node check its internal state - whether a running job is 
	 * finished for instance.
	 */
	public void run() {
		
		while (running) {
			// poll the nodes
			for (Node node : nodes)
				node.poll();
			
			// sleep
			try {
				Thread.sleep(pollSleep);
			} catch (InterruptedException ex) {
				assert(false) : "Cluster poll thread was interrupted";
			}
			
		}
		
	}

	/**
	 * Stops the polling thread. This must be called explicitly to make sure the program
	 * terminates cleanly.
	 */
	public void stopPollThread() {
		running = false;
		try {
			pollingThread.join();
		} catch (InterruptedException ex) {
			assert(false) : "Cluster stopPollThread was interrupted";
		}
		
	}
	
}
