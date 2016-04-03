package distributed.systems.gridscheduler.model;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
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
 * @author Niels Brouwers, Boaz Pat-El edited by Carlo van der Valk and Ka-Ping Wan
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
	public ResourceManager(Cluster cluster)	throws RemoteException{
		// preconditions
		assert(cluster != null);
		System.out.println("Creating ResourceManager: "+ cluster.getName());
		this.jobQueue = new ConcurrentLinkedQueue<Job>();

		this.cluster = cluster;
		this.socketURL = cluster.getName();
		completed = "";
		load = 0;
		time = new AtomicLong(0);
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
		Registry registry = LocateRegistry.getRegistry();
		GridSchedulerNodeInterface temp1 = (GridSchedulerNodeInterface) registry.lookup(gridSchedulerURL);
		temp1.onMessageReceived(notificationMessage);
		
		// if the jobqueue is full, offload the job to the grid scheduler
		if (jobQueue.size() >= jobQueueSize) {

			ControlMessage controlMessage = new ControlMessage(ControlMessageType.AddJob);
			controlMessage.setJob(job);
			controlMessage.setUrl(socketURL);
			registry = LocateRegistry.getRegistry();
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
			freeNode.startJob(waitingJob);
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
		System.out.println(job.toString());
		
		time.addAndGet( (long) (job.getTimeCompleted().getTime() - job.getTimeArrived().getTime() - job.getDuration()) );
		// job finished, remove it from our pool
		jobQueue.remove(job);
		completed += job.toString();
		completions.addAndGet(1);
		
		//notify gsn that job was completed
		ControlMessage notificationMessage = new ControlMessage(ControlMessageType.JobCompletion);
		notificationMessage.setUrl(socketURL);
		notificationMessage.setJob(job);
		
		Registry registry;
		try {
			registry = LocateRegistry.getRegistry();
			GridSchedulerNodeInterface temp = (GridSchedulerNodeInterface) registry.lookup(gridSchedulerURL);
			temp.onMessageReceived(notificationMessage);
		} catch (RemoteException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NotBoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
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
	public void connectToGridScheduler(String gridSchedulerURL) throws RemoteException, NotBoundException, InterruptedException {

		// preconditions
		assert(gridSchedulerURL != null) : "the parameter 'gridSchedulerURL' cannot be null"; 

		this.gridSchedulerURL = gridSchedulerURL;

		ControlMessage message = new ControlMessage(ControlMessageType.ResourceManagerJoin);
		message.setUrl(socketURL);
		message.setMax(jobQueueSize);
		
		Registry registry = LocateRegistry.getRegistry();
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

		// resource manager wants to offload a job to us 
		if (controlMessage.getType() == ControlMessageType.AddJob)
		{
			controlMessage.getJob().addToLog(socketURL);
			jobQueue.add(controlMessage.getJob());
			
			//send msg to gsn that job has arrived
			ControlMessage notificationMessage = new ControlMessage(ControlMessageType.JobArrival);
			notificationMessage.setJob(controlMessage.getJob());
			notificationMessage.setUrl(socketURL);
			notificationMessage.setFromCluster(true);
			Registry registry = LocateRegistry.getRegistry();
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
			Registry registry = LocateRegistry.getRegistry();
			GridSchedulerNodeInterface temp = (GridSchedulerNodeInterface) registry.lookup(gridSchedulerURL);
			temp.onMessageReceived(replyMessage);
			
			System.out.println(toString());
		}
	}
	
	public int getLoad(){
		return jobQueue.size();
	}
	
	public String toString(){
		if(completions.get() > 0 && time.get() > 0){
			return socketURL + ": " + " - " + completions.get() + " - " + (time.get() / completions.get());
		}
		return "";
	}

}
