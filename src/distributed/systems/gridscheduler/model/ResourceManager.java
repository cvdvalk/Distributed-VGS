package distributed.systems.gridscheduler.model;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;


/**
 * This class represents a resource manager in the VGS. It is a component of a cluster, 
 * and schedulers jobs to nodes on behalf of that cluster. It will offload jobs to the grid
 * scheduler if it has more jobs waiting in the queue than a certain amount.
 * 
 * The <i>jobQueueSize</i> is a variable that indicates the cutoff point. If there are more
 * jobs waiting for completion (including the ones that are running at one of the nodes)
 * than this variable, jobs are sent to the grid scheduler instead. This variable is currently
 * defaulted to [number of nodes] + MAX_QUEUE_SIZE. This means there can be at most MAX_QUEUE_SIZE jobs waiting 
 * locally for completion. 
 * 
 * Of course, this scheme is totally open to revision.
 * 
 * @author Niels Brouwers, Boaz Pat-El edited by Carlo van der Valk
 *
 */
public class ResourceManager extends UnicastRemoteObject implements INodeEventHandler, ResourceManagerInterface {
	private Cluster cluster;
	private Queue<Job> jobQueue;
	private String socketURL;
	private int jobQueueSize;
	public static final int MAX_QUEUE_SIZE = 32; 
	private String completed;
	private int load;
	private AtomicLong time;
	private AtomicInteger completions;
	private List<Long> pirateList;
	private String adress;
	private int port;
	private String nodeAdress;
	private int nodePort;
	private Date heartbeat;

	// Scheduler url
	private String gridSchedulerURL = null;


	/**
	 * Constructs a new ResourceManager object.
	 * <P> 
	 * <DL>
	 * <DT><B>Preconditions:</B>
	 * <DD>the parameter <CODE>cluster</CODE> cannot be null
	 * </DL>
	 * @param cluster the cluster to wich this resource manager belongs.
	 */
	public ResourceManager(Cluster cluster, String adress, int port)	throws RemoteException{
		// preconditions
		assert(cluster != null);
		System.out.println("Creating ResourceManager: "+ cluster.getName());
		this.jobQueue = new ConcurrentLinkedQueue<Job>();
		pirateList = Collections.synchronizedList(new ArrayList<Long>());
		this.cluster = cluster;
		this.socketURL = cluster.getName();
		completed = "";
		load = 0;
		time = new AtomicLong(0);
		this.port =port;
		this.adress = adress;
		// Number of jobs in the queue must be larger than the number of nodes, because
		// jobs are kept in queue until finished. The queue is a bit larger than the 
		// number of nodes for efficiency reasons - when there are only a few more jobs than
		// nodes we can assume a node will become available soon to handle that job.
		jobQueueSize = cluster.getNodeCount() + MAX_QUEUE_SIZE;
		completions = new AtomicInteger(0);
	}

	/**
	 * Add a job to the resource manager. If there is a free node in the cluster the job will be
	 * scheduled onto that Node immediately. If all nodes are busy the job will be put into a local
	 * queue. If the local queue is full, the job will be offloaded to the grid scheduler.
	 * <DL>
	 * <DT><B>Preconditions:</B>
	 * <DD>the parameter <CODE>job</CODE> cannot be null
	 * <DD>a grid scheduler url has to be set for this rm before calling this function (the RM has to be
	 * connected to a grid scheduler)
	 * </DL>
	 * @param job the Job to run
	 * @throws RemoteException 
	 * @throws NotBoundException 
	 * @throws InterruptedException 
	 */
	public void addJob(Job job) throws RemoteException, NotBoundException, InterruptedException {
		// check preconditions
		assert(job != null) : "the parameter 'job' cannot be null";
		assert(gridSchedulerURL != null) : "No grid scheduler URL has been set for this resource manager";
		job.setLog(socketURL);
		job.setLast(socketURL);
		job.setTimeArrived();
		
		//send msg to gsn that job has arrived
		ControlMessage notificationMessage = new ControlMessage(ControlMessageType.JobArrival);
		notificationMessage.setJob(job);
		notificationMessage.setUrl(socketURL);
		notificationMessage.setAdress(adress);
		notificationMessage.setPort(port);
		notificationMessage.setFromCluster(true);
		notificationMessage.setTimestamp(new Date());
		
//		System.setProperty("java.rmi.server.hostname", nodeAdress); 
		Registry registry = LocateRegistry.getRegistry(nodeAdress,nodePort);
		GridSchedulerNodeInterface temp1 = (GridSchedulerNodeInterface) registry.lookup(gridSchedulerURL);
		temp1.onMessageReceived(notificationMessage);
		if(new Date().getTime() - heartbeat.getTime() > 5000){
			//GSN is down
			jobQueue.add(job);
			scheduleJobs();
		}
		// if the jobqueue is full, offload the job to the grid scheduler
		else if (jobQueue.size() >= jobQueueSize) {

			ControlMessage controlMessage = new ControlMessage(ControlMessageType.AddJob);
			controlMessage.setJob(job);
			controlMessage.setUrl(socketURL);
			controlMessage.setAdress(adress);
			controlMessage.setPort(port);
			
			registry = LocateRegistry.getRegistry(nodeAdress,nodePort);
			GridSchedulerNodeInterface temp2 = (GridSchedulerNodeInterface) registry.lookup(gridSchedulerURL);
			temp2.onMessageReceived(controlMessage);

			// otherwise store it in the local queue
		} else {
			jobQueue.add(job);
			scheduleJobs();
		}

	}

	/**
	 * Tries to find a waiting job in the jobqueue.
	 * @return
	 */
	public Job getWaitingJob() {
		// find a waiting job
		for (Job job : jobQueue) {
			if (job.getStatus() == JobStatus.Waiting){
				return job;
			}
		}
		// no waiting jobs found, return null
		return null;
	}

	/**
	 * Tries to schedule jobs in the jobqueue to free nodes. 
	 */
	public void scheduleJobs() {
		// while there are jobs to do and we have nodes available, assign the jobs to the 
		// free nodes
		Node freeNode;
		Job waitingJob;
		
		while ( ((waitingJob = getWaitingJob()) != null) && ((freeNode = cluster.getFreeNode()) != null) ) {
			//send msg to gsn that job has arrived
			ControlMessage notificationMessage = new ControlMessage(ControlMessageType.JobStart);
			notificationMessage.setJob(waitingJob);
			notificationMessage.setUrl(socketURL);
			notificationMessage.setAdress(adress);
			notificationMessage.setPort(port);
			notificationMessage.setFromCluster(true);
			notificationMessage.setTimestamp(new Date());
			
			Registry registry;
			try {
				registry = LocateRegistry.getRegistry(nodeAdress,nodePort);
				GridSchedulerNodeInterface temp1 = (GridSchedulerNodeInterface) registry.lookup(gridSchedulerURL);
				temp1.onMessageReceived(notificationMessage);
				
				freeNode.startJob(waitingJob);
			} catch (RemoteException e) {
			} catch (NotBoundException e) {
			} catch (InterruptedException e) {
			}
		}
	}

	/**
	 * Called when a job is finished
	 * <p>
	 * pre: parameter 'job' cannot be null
	 */
	public void jobDone(Job job) {
		// preconditions
		assert(job != null) : "parameter 'job' cannot be null";
		job.setTimeCompleted();
//		System.out.println(job.toString());
		long t = (long) (job.getTimeCompleted().getTime() - job.getTimeArrived().getTime() - job.getDuration());
		time.addAndGet( t );
		// job finished, remove it from our pool
		jobQueue.remove(job);
		completed += job.toString();
		completions.addAndGet(1);
		pirateList.add(t);
		if(jobQueue.size() == 0){
			System.out.println(toString());
		}
		
		//notify gsn that job was completed
		ControlMessage notificationMessage = new ControlMessage(ControlMessageType.JobCompletion);
		notificationMessage.setUrl(socketURL);
		notificationMessage.setAdress(adress);
		notificationMessage.setPort(port);
		notificationMessage.setJob(job);
		notificationMessage.setFromCluster(true);
		notificationMessage.setTimestamp(new Date());
		
		Registry registry;
		try {
//			System.setProperty("java.rmi.server.hostname", nodeAdress); 
			registry = LocateRegistry.getRegistry(nodeAdress,nodePort);
			GridSchedulerNodeInterface temp = (GridSchedulerNodeInterface) registry.lookup(gridSchedulerURL);
			temp.onMessageReceived(notificationMessage);
		} catch (RemoteException e) {
			//e.printStackTrace();
		} catch (NotBoundException e) {
			//e.printStackTrace();
		} catch (InterruptedException e) {
			//e.printStackTrace();
		}
		catch(Exception e){
			
		}
		
	}

	/**
	 * @return the url of the grid scheduler this RM is connected to 
	 */
	public String getGridSchedulerURL() {
		return gridSchedulerURL;
	}

	/**
	 * Connect to a grid scheduler
	 * <p>
	 * pre: the parameter 'gridSchedulerURL' must not be null
	 * @param gridSchedulerURL
	 * @throws RemoteException 
	 * @throws NotBoundException 
	 * @throws InterruptedException 
	 */
	public void connectToGridScheduler(String gridSchedulerURL, String nodeAdress, int nodePort) throws RemoteException, NotBoundException, InterruptedException {

		// preconditions
		assert(gridSchedulerURL != null) : "the parameter 'gridSchedulerURL' cannot be null"; 

		this.gridSchedulerURL = gridSchedulerURL;
		this.nodeAdress=nodeAdress;
		this.nodePort=nodePort;
		ControlMessage message = new ControlMessage(ControlMessageType.ResourceManagerJoin);
		message.setUrl(socketURL);
		message.setAdress(adress);
		message.setPort(port);
		message.setMax(jobQueueSize);
		heartbeat = new Date();
//		System.setProperty("java.rmi.server.hostname", nodeAdress); 
		Registry registry = LocateRegistry.getRegistry(nodeAdress,nodePort);
		GridSchedulerNodeInterface temp = (GridSchedulerNodeInterface) registry.lookup(gridSchedulerURL);
		temp.onMessageReceived(message);
	}

	/**
	 * Message received handler
	 * <p>
	 * pre: parameter 'message' should be of type ControlMessage 
	 * pre: parameter 'message' should not be null 
	 * @param message a message
	 * @throws RemoteException 
	 * @throws NotBoundException 
	 * @throws InterruptedException 
	 */
	public void onMessageReceived(ControlMessage message) throws RemoteException, NotBoundException, InterruptedException {
		// preconditions
		assert(message instanceof ControlMessage) : "parameter 'message' should be of type ControlMessage";
		assert(message != null) : "parameter 'message' cannot be null";

		ControlMessage controlMessage = (ControlMessage)message;
		heartbeat = new Date();
		// gsn wants to offload a job to us 
		if (controlMessage.getType() == ControlMessageType.AddJob)
		{
			controlMessage.getJob().addToLog(socketURL);
			jobQueue.add(controlMessage.getJob());
			
			//send msg to gsn that job has arrived
			ControlMessage notificationMessage = new ControlMessage(ControlMessageType.JobArrival);
			notificationMessage.setJob(controlMessage.getJob());
			notificationMessage.setUrl(socketURL);
			notificationMessage.setAdress(adress);
			notificationMessage.setPort(port);
			notificationMessage.setFromCluster(true);
			
			Registry registry = LocateRegistry.getRegistry(nodeAdress,nodePort);
			GridSchedulerNodeInterface temp1 = (GridSchedulerNodeInterface) registry.lookup(gridSchedulerURL);
			temp1.onMessageReceived(notificationMessage);
			
			scheduleJobs();
			
			
		}
		// 
		if (controlMessage.getType() == ControlMessageType.RequestLoad)
		{
			
			scheduleJobs();
			ControlMessage replyMessage = new ControlMessage(ControlMessageType.ReplyLoad);
			replyMessage.setUrl(cluster.getName());
			replyMessage.setLoad(jobQueue.size());
			replyMessage.setAdress(adress);
			replyMessage.setPort(port);
			
			Registry registry = LocateRegistry.getRegistry(nodeAdress,nodePort);
			GridSchedulerNodeInterface temp = (GridSchedulerNodeInterface) registry.lookup(gridSchedulerURL);
			temp.onMessageReceived(replyMessage);
//			if(controlMessage.getUrl().equals("Node3")){
//				System.out.println("Node3 lives");
//			}
		}
		//
//		if(Math.random() * 1000 <= 1){
//			System.out.println(socketURL + ": crashed" );
//			Crash();
//			return;
//		}
		//
		//
		if(controlMessage.getType() == ControlMessageType.HeartBeat){
			ControlMessage cMessage = new ControlMessage(ControlMessageType.HeartBeatReply);
			cMessage.setUrl(socketURL);
			cMessage.setAdress(adress);
			cMessage.setPort(port);
			cMessage.setFromCluster(true);
			Registry registry;
			try {
				registry = LocateRegistry.getRegistry(controlMessage.getAdress(),controlMessage.getPort());
				GridSchedulerNodeInterface temp = (GridSchedulerNodeInterface) registry.lookup(controlMessage.getUrl());
				temp.onMessageReceived(cMessage);
			} catch (RemoteException e) {
				//e.printStackTrace();
			} catch (NotBoundException e) {
				//e.printStackTrace();
			} catch (InterruptedException e) {
				//e.printStackTrace();
			}
			catch(Exception e){
				
			}
		}
	}
	
	public int getLoad(){
		return jobQueue.size();
	}
	
	public String toString(){
		if(completions.get() > 0 && time.get() > 0 && pirateList.size() > 3){
			long[] test = new long[pirateList.size()];
			int i = 0;
			String all_data  = "";
			for(long pirate: pirateList){
				test[i] = pirate;
				i++;
				all_data += "," + pirate;
			}
			Arrays.sort(test);
			long min = test[0];
			long max = test[pirateList.size()-1];
			int middle = ((test.length) / 2);
			long median = getMedian(test);
			long q1 = getMedian(Arrays.copyOfRange(test, 0, middle));
			long q3 = getMedian(Arrays.copyOfRange(test, middle, pirateList.size()-1));
			long average = time.get() / completions.get();
			
			all_data  = socketURL + "=[" + all_data + "]";
			
//			return all_data;
			return socketURL + "=" + pirateList.size();
//			return socketURL + "," + "" + completions.get() 
//					+ "," + average 
//					+ "," + min
//					+ "," + q1
//					+ "," + median
//					+ "," + q3
//					+ "," + max;
		}
		return "";
	}
	
	public void Crash(){
		this.jobQueue = new ConcurrentLinkedQueue<Job>();
		
		ControlMessage replyMessage = new ControlMessage(ControlMessageType.NodeStart);
		replyMessage.setUrl(socketURL);
		replyMessage.setAdress(adress);
		replyMessage.setPort(port);
		replyMessage.setFromCluster(true);
		Registry registry;
		try {
			registry = LocateRegistry.getRegistry(nodeAdress,nodePort);
			GridSchedulerNodeInterface temp = (GridSchedulerNodeInterface) registry.lookup(gridSchedulerURL);
			temp.onMessageReceived(replyMessage);
		} catch (RemoteException e) {
			//e.printStackTrace();
		} catch (NotBoundException e) {
			//e.printStackTrace();
		} catch (InterruptedException e) {
			//e.printStackTrace();
		}
		catch(Exception e){
			
		}
	}
	
	private long getMedian(long[] in){
		long median;
		if (in.length % 2 == 0){
		    median = ((long)in[in.length/2] + (long)in[in.length/2 - 1])/2;
		}
		else{
		    median = (long) in[in.length/2];
		}
		return median;
	}

}
