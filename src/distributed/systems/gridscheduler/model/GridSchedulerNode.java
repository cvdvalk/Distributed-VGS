package distributed.systems.gridscheduler.model;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * 
 * The GridScheduler class represents a single-server implementation of the grid scheduler in the
 * virtual grid system.
 * 
 * @author Niels Brouwers, edited by Carlo van der Valk and Ka-Ping Wan
 *
 */
public class GridSchedulerNode extends UnicastRemoteObject implements Runnable, GridSchedulerNodeInterface {
	
	// job queue
	private ConcurrentLinkedQueue<Job> jobQueue;
	
	// local url
	private final String url;
	private List<String> gridScheduler;

	// a hashmap linking each resource manager to an estimated load
	private ConcurrentHashMap<String, Integer> resourceManagerLoad;
	private ConcurrentHashMap<String, Integer> resourceManagerLoadMax;
	private ConcurrentHashMap<String, Integer> NodeLoad;
	private ConcurrentHashMap<String, Integer> completion;
	private ArrayList<ControlMessage> comp;
	// polling frequency, 1hz
	private long pollSleep = 1000;
	
	// polling thread
	private Thread pollingThread;
	private boolean running;
	
	/**
	 * Constructs a new GridScheduler object at a given url.
	 * <p>
	 * <DL>
	 * <DT><B>Preconditions:</B>
	 * <DD>parameter <CODE>url</CODE> cannot be null
	 * </DL>
	 * @param url the gridscheduler's url to register at
	 */
	public GridSchedulerNode(String url) throws RemoteException{
		// preconditions
		assert(url != null) : "parameter 'url' cannot be null";
		
		// init members
		this.url = url;
		this.resourceManagerLoad = new ConcurrentHashMap<String, Integer>();
		this.resourceManagerLoadMax = new ConcurrentHashMap<String, Integer>();
		this.NodeLoad = new ConcurrentHashMap<String, Integer>();
		this.jobQueue = new ConcurrentLinkedQueue<Job>();
		this.completion = new ConcurrentHashMap<String, Integer>();
		this.gridScheduler = new ArrayList<String>();
		comp = new ArrayList();

		// start the polling thread
		running = true;
		pollingThread = new Thread(this);
		pollingThread.start();
		System.out.println("Creating GSN: " + url);
	}
	
	/**
	 * The gridscheduler's name also doubles as its URL in the local messaging system.
	 * It is passed to the constructor and cannot be changed afterwards.
	 * @return the name of the gridscheduler
	 */
	public String getUrl() {
		return url;
	}

	/**
	 * Gets the number of jobs that are waiting for completion.
	 * @return
	 */
	public int getWaitingJobs() {
		int ret = 0;
		ret = jobQueue.size();
		return ret;
	}

	/**
	 * Receives a message from another component.
	 * <p>
	 * <DL>
	 * <DT><B>Preconditions:</B>
	 * <DD>parameter <CODE>message</CODE> should be of type ControlMessage 
	 * <DD>parameter <CODE>message</CODE> should not be null
	 * </DL> 
	 * @param message a message
	 * @throws InterruptedException 
	 */
	public void onMessageReceived(ControlMessage message) throws InterruptedException {
		// preconditions
		assert(message instanceof ControlMessage) : "parameter 'message' should be of type ControlMessage";
		assert(message != null) : "parameter 'message' cannot be null";
		
		ControlMessage controlMessage = (ControlMessage)message;
		
		// resource manager wants to join this grid scheduler 
		// when a new RM is added, its load is set to Integer.MAX_VALUE to make sure
		// no jobs are scheduled to it until we know the actual load
		if (controlMessage.getType() == ControlMessageType.ResourceManagerJoin){
			resourceManagerLoad.put(controlMessage.getUrl(), Integer.MAX_VALUE);
			resourceManagerLoadMax.put(controlMessage.getUrl(), controlMessage.getMax());
			completion.put(controlMessage.getUrl(), 0);
		}
		if (controlMessage.getType() == ControlMessageType.GridSchedulerNodeJoin){
			gridScheduler.add(controlMessage.getUrl());
			NodeLoad.put(controlMessage.getUrl(), Integer.MAX_VALUE);
		}
		// resource manager wants to offload a job to us 
		if (controlMessage.getType() == ControlMessageType.AddJob){
			if(!hasJob(controlMessage.getJob())){
				controlMessage.getJob().addToLog(url);
				controlMessage.getJob().setLast(url);
				jobQueue.add(controlMessage.getJob());
			}
		}
		// 
		if (controlMessage.getType() == ControlMessageType.ReplyLoad){
			resourceManagerLoad.put(controlMessage.getUrl(),controlMessage.getLoad());
		}
		// 
		if (controlMessage.getType() == ControlMessageType.NodeStart){
		
		}
		//
		if(controlMessage.getType() == ControlMessageType.RequestLoad){
			ControlMessage cMessage = new ControlMessage(ControlMessageType.ReplyToNode);
			cMessage.setUrl(this.getUrl());
			int load_temp = Integer.MAX_VALUE;
			if(getLeastLoadedRM() != null){
				load_temp = resourceManagerLoad.get(getLeastLoadedRM());
			}
			cMessage.setLoad(load_temp);
			Registry registry;
			try {
				registry = LocateRegistry.getRegistry();
				GridSchedulerNodeInterface temp = (GridSchedulerNodeInterface) registry.lookup(controlMessage.getUrl());
				temp.onMessageReceived(cMessage);
			} catch (RemoteException e) {
				e.printStackTrace();
			} catch (NotBoundException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		//
		if(controlMessage.getType() == ControlMessageType.ReplyToNode){
			NodeLoad.put(controlMessage.getUrl(),controlMessage.getLoad());
		}
		//
		if(controlMessage.getType() == ControlMessageType.JobCompletion){
			String tm_temp = controlMessage.getUrl();
			int load_temp = completion.get(tm_temp);
			load_temp++;
			completion.put(tm_temp, load_temp);
			comp.add(controlMessage);
		}
		
	}

	// finds the least loaded resource manager and returns its url
	private String getLeastLoadedRM() {
		String ret = null; 
		int minLoad = Integer.MAX_VALUE;
		
		// loop over all resource managers, and pick the one with the lowest load
		for (String key : resourceManagerLoad.keySet())
		{
			if (resourceManagerLoad.get(key) <= minLoad &&  resourceManagerLoad.get(key) < resourceManagerLoadMax.get(key))
			{
				ret = key;
				minLoad = resourceManagerLoad.get(key);
			}
		}
		return ret;		
	}
	
	// finds the least loaded resource manager and returns its url
		private String getLeastLoadedNode() {
			String ret = null; 
			int minLoad = Integer.MAX_VALUE;
			
			// loop over all resource managers, and pick the one with the lowest load
			for (String key : NodeLoad.keySet())
			{
//				System.out.println(key);
				if (NodeLoad.get(key) <= minLoad)
				{
					ret = key;
					minLoad = NodeLoad.get(key);
				}
			}
			return ret;		
		}

	/**
	 * Polling thread runner. This thread polls each resource manager in turn to get its load,
	 * then offloads any job in the waiting queue to that resource manager
	 */
	public void run() {
		while (running) {
			// send a message to each resource manager, requesting its load
			for (String rmUrl : resourceManagerLoad.keySet())
			{
				ControlMessage cMessage = new ControlMessage(ControlMessageType.RequestLoad);
				cMessage.setUrl(this.getUrl());
				Registry registry;
				try {
					registry = LocateRegistry.getRegistry();
					ResourceManagerInterface temp = (ResourceManagerInterface) registry.lookup(rmUrl);
					temp.onMessageReceived(cMessage);
				} catch (RemoteException e) {
					e.printStackTrace();
				} catch (NotBoundException e) {
					e.printStackTrace();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
			}
			for (String rmUrl : NodeLoad.keySet())
			{
				ControlMessage cMessage = new ControlMessage(ControlMessageType.RequestLoad);
				cMessage.setUrl(this.getUrl());
				Registry registry;
				try {
					registry = LocateRegistry.getRegistry();
					GridSchedulerNodeInterface temp = (GridSchedulerNodeInterface) registry.lookup(rmUrl);
					temp.onMessageReceived(cMessage);
				} catch (RemoteException e) {
					e.printStackTrace();
				} catch (NotBoundException e) {
					e.printStackTrace();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
			}
			
			// schedule waiting messages to the different clusters
			for (Job job : jobQueue)
			{
				String leastLoadedRM =  getLeastLoadedRM();
				String leastLoadedNode =  getLeastLoadedNode();
				Registry registry;
//				if(job.isNodeToNode() && leastLoadedRM!=null){
//					ControlMessage cMessage = new ControlMessage(ControlMessageType.AddJob);
//					cMessage.setJob(job);
//					
//					try {
//						registry = LocateRegistry.getRegistry();
//						ResourceManagerInterface temp = (ResourceManagerInterface) registry.lookup(leastLoadedRM);
//						temp.onMessageReceived(cMessage);
//						
//						
//					} catch (RemoteException e) {
//						e.printStackTrace();
//					} catch (NotBoundException e) {
//						e.printStackTrace();
//					} catch (InterruptedException e) {
//						// TODO Auto-generated catch block
//						e.printStackTrace();
//					}
//				
//					jobQueue.remove(job);
//					
//					// increase the estimated load of that RM by 1 (because we just added a job)
//					int load = resourceManagerLoad.get(leastLoadedRM);
//					resourceManagerLoad.put(leastLoadedRM, load+1);
//				}
//				
//				else 
				if (leastLoadedRM!=null) {
					
					
					if( (resourceManagerLoad.get(leastLoadedRM) < NodeLoad.get(leastLoadedNode)) ){
						ControlMessage cMessage = new ControlMessage(ControlMessageType.AddJob);
						cMessage.setJob(job);
						
						try {
							registry = LocateRegistry.getRegistry();
							ResourceManagerInterface temp = (ResourceManagerInterface) registry.lookup(leastLoadedRM);
							temp.onMessageReceived(cMessage);
							
							
						} catch (RemoteException e) {
							e.printStackTrace();
						} catch (NotBoundException e) {
							e.printStackTrace();
						} catch (InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					
						jobQueue.remove(job);
						
						// increase the estimated load of that RM by 1 (because we just added a job)
						int load = resourceManagerLoad.get(leastLoadedRM);
						resourceManagerLoad.put(leastLoadedRM, load+1);
					}
					else{
						try {
							registry = LocateRegistry.getRegistry();
							if (leastLoadedNode!=null) {
								ControlMessage cMessage = new ControlMessage(ControlMessageType.AddJob);
								job.setNodeToNode();
								cMessage.setJob(job);
								
								GridSchedulerNodeInterface temp = (GridSchedulerNodeInterface) registry.lookup(leastLoadedNode);
								temp.onMessageReceived(cMessage);
								
								jobQueue.remove(job);
								
								jobQueue.remove(job);
								int load = NodeLoad.get(leastLoadedNode);
								NodeLoad.put(leastLoadedNode, load+1);
							}
							
						} catch (RemoteException e) {
							e.printStackTrace();
						} catch (NotBoundException e) {
							e.printStackTrace();
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
					}
					
				}
//				else{
//					try {
//						registry = LocateRegistry.getRegistry();
//						if (leastLoadedNode!=null) {
//							ControlMessage cMessage = new ControlMessage(ControlMessageType.AddJob);
//							cMessage.setJob(job);
//							
//							GridSchedulerNodeInterface temp = (GridSchedulerNodeInterface) registry.lookup(leastLoadedNode);
//							temp.onMessageReceived(cMessage);
//							
//							jobQueue.remove(job);
//							int load = NodeLoad.get(leastLoadedNode);
//							NodeLoad.put(leastLoadedNode, load+1);
//						}
//						
//					} catch (RemoteException e) {
//						e.printStackTrace();
//					} catch (NotBoundException e) {
//						e.printStackTrace();
//					} catch (InterruptedException e) {
//						e.printStackTrace();
//					}
//				}
				
				
			}
			
			// sleep
			try
			{
				Thread.sleep(pollSleep);
			} catch (InterruptedException ex) {
				assert(false) : "Grid scheduler runtread was interrupted";
			}
			
		}
		
	}
	
	/**
	 * Stop the polling thread. This has to be called explicitly to make sure the program 
	 * terminates cleanly.
	 *
	 */
	public void stopPollThread() {
		running = false;
		try {
			pollingThread.join();
		} catch (InterruptedException ex) {
			assert(false) : "Grid scheduler stopPollThread was interrupted";
		}
		
	}
	
	public void broadcastNodeStart(){
		//send nodestart message
	}
	
	public void NodeCrash() throws InterruptedException{
		//crash node, clear most data, sleep, restart, send restart msg
		running = false;
		Thread.sleep(5000);
		//for each gsn, send msg
		//they send addjob messages?
		running = true;
	}
	
	public void connectToGridScheduler(String gridSchedulerURL) throws RemoteException, NotBoundException, InterruptedException {

		// preconditions
		assert(gridSchedulerURL != null) : "the parameter 'gridSchedulerURL' cannot be null"; 

		gridScheduler.add(gridSchedulerURL);
		NodeLoad.put(gridSchedulerURL, Integer.MAX_VALUE);
		ControlMessage message = new ControlMessage(ControlMessageType.GridSchedulerNodeJoin);
		message.setUrl(url);
		
		Registry registry = LocateRegistry.getRegistry();
		GridSchedulerNodeInterface temp = (GridSchedulerNodeInterface) registry.lookup(gridSchedulerURL);
		temp.onMessageReceived(message);

	}
	
	public boolean hasJob(Job job){
		Iterator<Job> iter = jobQueue.iterator();
		while(iter.hasNext()){
			Job current = iter.next();
			if(current.getId() == job.getId()){
				return true;
			}
		}
		return false;
	}
	
	public String toString(){
		String result = "";
//		for (String key : completion.keySet())
//		{
//			result += "["+ key + ": " + completion.get(key) + "]";
//		}
		return url + ": " + comp.size();
	}
	
}
