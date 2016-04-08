package distributed.systems.gridscheduler.model;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.rmi.AlreadyBoundException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 
 * The GridScheduler class represents a single-server implementation of the grid scheduler in the
 * virtual grid system.
 * 
 * @author Niels Brouwers, edited by Carlo van der Valk
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
	private ConcurrentHashMap<String, Integer> port;
	private ConcurrentHashMap<String, String> adress;
	private ConcurrentHashMap<String, Date> heartbeat_hashmap;
	private ConcurrentHashMap<Long, String> job_log;
	private ConcurrentHashMap<Long, Job> job_log2;
	private ConcurrentHashMap<Long, Integer> elect;
	private ConcurrentHashMap<String, Date> some_log;
	private ConcurrentHashMap<String, Boolean> node_life;
	private AtomicInteger active_nodes;
	// polling frequency, 1hz
	private long pollSleep = 1000;
	
	// polling thread
	private Thread pollingThread;
	private boolean running;
	private String adrr;
	private int portnr;
	
	/**
	 * Constructs a new GridScheduler object at a given url.
	 * <p>
	 * <DL>
	 * <DT><B>Preconditions:</B>
	 * <DD>parameter <CODE>url</CODE> cannot be null
	 * </DL>
	 * @param url the gridscheduler's url to register at
	 * @throws AlreadyBoundException 
	 */
	public GridSchedulerNode(String url, String adress, int port) throws RemoteException, AlreadyBoundException{
		// preconditions
		assert(url != null) : "parameter 'url' cannot be null";
		
		try {
			System.setProperty("java.rmi.server.hostname", adress); 
			java.rmi.registry.LocateRegistry.createRegistry(port);
		} catch (RemoteException e) {
			//e.printStackTrace();
		}
		Registry registry = LocateRegistry.getRegistry(port);
		// init members
		this.url = url;
		this.adrr = adress;
		this.portnr = port;
		this.resourceManagerLoad = new ConcurrentHashMap<String, Integer>();
		this.resourceManagerLoadMax = new ConcurrentHashMap<String, Integer>();
		this.NodeLoad = new ConcurrentHashMap<String, Integer>();
		this.jobQueue = new ConcurrentLinkedQueue<Job>();
		this.completion = new ConcurrentHashMap<String, Integer>();
		this.port = new ConcurrentHashMap<String, Integer>();
		this.adress = new ConcurrentHashMap<String, String>();
		this.gridScheduler = new ArrayList<String>();
		elect = new ConcurrentHashMap<Long, Integer>();
		heartbeat_hashmap = new ConcurrentHashMap<String, Date>();
		some_log = new ConcurrentHashMap<String, Date>();
		job_log = new ConcurrentHashMap<Long, String>();
		job_log2 = new ConcurrentHashMap<Long, Job>();
		// start the polling thread
		running = true;
		pollingThread = new Thread(this);
		pollingThread.start();
		registry.bind(url, this);
		active_nodes = new AtomicInteger(1);
		node_life = new ConcurrentHashMap<String, Boolean>();
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
		
		if (controlMessage.getType() == ControlMessageType.HeartBeatReply){
			if(node_life.get(controlMessage.getUrl())!=null){
				
				if(!node_life.get(controlMessage.getUrl()) && !controlMessage.fromCluster()){
					active_nodes.addAndGet(1);
					//node is back alive
					append(url+".txt", url + "- "+ new Date().toString()+ ": " + controlMessage.getUrl() + " is alive again");
					//TODO broadcast node alive
				}
			}
			node_life.put(controlMessage.getUrl(), true);
		}
		// resource manager wants to join this grid scheduler 
		// when a new RM is added, its load is set to Integer.MAX_VALUE to make sure
		// no jobs are scheduled to it until we know the actual load
		if (controlMessage.getType() == ControlMessageType.ResourceManagerJoin){
			resourceManagerLoad.put(controlMessage.getUrl(), Integer.MAX_VALUE);
			resourceManagerLoadMax.put(controlMessage.getUrl(), controlMessage.getMax());
			this.port.put(controlMessage.getUrl(), controlMessage.getPort());
			this.adress.put(controlMessage.getUrl(), controlMessage.getAdress());
			completion.put(controlMessage.getUrl(), 0);
			heartbeat_hashmap.put(controlMessage.getUrl(), new Date());
			System.out.println("added cluster: "+controlMessage.getUrl());
		}
		//couple two GSN
		if (controlMessage.getType() == ControlMessageType.GridSchedulerNodeJoin){
			gridScheduler.add(controlMessage.getUrl());
			this.port.put(controlMessage.getUrl(), controlMessage.getPort());
			this.adress.put(controlMessage.getUrl(), controlMessage.getAdress());
			heartbeat_hashmap.put(controlMessage.getUrl(), new Date());
			NodeLoad.put(controlMessage.getUrl(), Integer.MAX_VALUE);
			System.out.println("added node: "+controlMessage.getUrl());
		}
		// resource manager wants to offload a job to us 
		if (controlMessage.getType() == ControlMessageType.AddJob){
			if(!hasJob(controlMessage.getJob())){
				Date timestamp = new Date();
				append(url+".txt", url + "- "+ timestamp.toString()
						+ ": Job " + controlMessage.getJob().getId() + " arrived at " + url);
				
				controlMessage.getJob().addToLog(url);
				controlMessage.getJob().setLast(url);
				jobQueue.add(controlMessage.getJob());
				
				//check if job is in message log
				job_log.put(controlMessage.getJob().getId(), controlMessage.getUrl());
				job_log2.put(controlMessage.getJob().getId(), controlMessage.getJob());
				
				//add to log
//				some_log.put(controlMessage.getJob().toString(), new Date());
				
				//jobArrived broadcast
				for (String bro_url : gridScheduler)
				{
					ControlMessage cMessage = new ControlMessage(ControlMessageType.JobArrival);
					cMessage.setUrl(this.getUrl());
					cMessage.setAdress(this.adrr);
					cMessage.setPort(portnr);
					cMessage.setJob(controlMessage.getJob());
					cMessage.setTimestamp(timestamp);
					Registry registry;
					try {
						registry = LocateRegistry.getRegistry(adress.get(bro_url),port.get(bro_url));
						GridSchedulerNodeInterface temp = (GridSchedulerNodeInterface) registry.lookup(bro_url);
						temp.onMessageReceived(cMessage);
					} catch (RemoteException e) {
						////e.printStackTrace();
					} catch (NotBoundException e) {
						//e.printStackTrace();
					} catch (InterruptedException e) {
						//e.printStackTrace();
					} 
					catch(Exception e){
						
					}
				}
				
			}
		}
		//receive the load to other RM
		if (controlMessage.getType() == ControlMessageType.ReplyLoad){
			resourceManagerLoad.put(controlMessage.getUrl(),controlMessage.getLoad());
			heartbeat_hashmap.put(controlMessage.getUrl(), new Date());
//			System.out.println("got load " + controlMessage.getLoad() + " from " + controlMessage.getUrl());
		}
		//receive load request from other GSN 
		if(controlMessage.getType() == ControlMessageType.RequestLoad){
			heartbeat_hashmap.put(controlMessage.getUrl(), new Date());
			ControlMessage cMessage = new ControlMessage(ControlMessageType.ReplyToNode);
			cMessage.setUrl(this.getUrl());
			cMessage.setAdress(this.adrr);
			cMessage.setPort(this.portnr);
			int load_temp = Integer.MAX_VALUE;
//			int load_max = 1;
			if(getLeastLoadedRM() != null){
				load_temp = resourceManagerLoad.get(getLeastLoadedRM());
//				load_max = resourceManagerLoadMax.get(getLeastLoadedRM());
			}
			cMessage.setLoad(load_temp);
			//cMessage.setMax(load_max);
			Registry registry;
			try {
//				System.setProperty("java.rmi.server.hostname", adress.get(controlMessage.getUrl())); 
				registry = LocateRegistry.getRegistry(adress.get(controlMessage.getUrl()),controlMessage.getPort());
				GridSchedulerNodeInterface temp = (GridSchedulerNodeInterface) registry.lookup(controlMessage.getUrl());
				temp.onMessageReceived(cMessage);
			} catch (RemoteException e) {
				//e.printStackTrace();
			} catch (NotBoundException e) {
//				//e.printStackTrace();
//				System.out.println(controlMessage.getUrl() + " has crashed");
//				NodeLoad.put(controlMessage.getUrl(), Integer.MAX_VALUE);
			} catch (InterruptedException e) {
				//e.printStackTrace();
			}
			catch(Exception e){
				
			}
		}
		//receive the load to other RM
		if(controlMessage.getType() == ControlMessageType.ReplyToNode){
			heartbeat_hashmap.put(controlMessage.getUrl(), new Date());
			NodeLoad.put(controlMessage.getUrl(),controlMessage.getLoad());
//			System.out.println("got load " + controlMessage.getLoad() + " from " + controlMessage.getUrl());
		}
		//get message that a job has been completed
		if(controlMessage.getType() == ControlMessageType.JobCompletion){
			heartbeat_hashmap.put(controlMessage.getUrl(), new Date());
			
			append(url+".txt", controlMessage.getUrl() + "- "+ controlMessage.getTimestamp().toString()
					+ ": Job " + controlMessage.getJob().getId() + " completed at " + controlMessage.getUrl());
			
			if(job_log.get(controlMessage.getJob().getId())!=null){
				job_log.remove(controlMessage.getJob().getId());
				job_log2.remove(controlMessage.getJob().getId());
			}
			System.out.println(controlMessage.getJob().toString());
			//check if received from rm, if so broadcast to gsn
			if(controlMessage.fromCluster()){
				
				//broadcast
				controlMessage.setFromCluster(false);
				for (String bro_url : gridScheduler)
				{
					Registry registry;
					try {
//						System.setProperty("java.rmi.server.hostname", adress.get(bro_url)); 
						registry = LocateRegistry.getRegistry(adress.get(bro_url),port.get(bro_url));
						GridSchedulerNodeInterface temp = (GridSchedulerNodeInterface) registry.lookup(bro_url);
						temp.onMessageReceived(controlMessage);
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
		}
		//get message that a job has arrived at a gsn or rm
		if(controlMessage.getType() == ControlMessageType.JobArrival){
			heartbeat_hashmap.put(controlMessage.getUrl(), new Date());
			job_log.put(controlMessage.getJob().getId(), controlMessage.getUrl());
			job_log2.put(controlMessage.getJob().getId(), controlMessage.getJob());
			append(url+".txt", controlMessage.getUrl() + "- "+ controlMessage.getTimestamp().toString()
					+ ": Job " + controlMessage.getJob().getId() + " arrived at " + controlMessage.getUrl());
			//check if from rm, if so then broadcast
			//this is so that a job arriving on a rm gets logged on gsn
			if(controlMessage.fromCluster()){
				//broadcast
				controlMessage.setFromCluster(false);
				for (String bro_url : gridScheduler)
				{
					Registry registry;
					try {
						registry = LocateRegistry.getRegistry(adress.get(bro_url), port.get(bro_url));
						GridSchedulerNodeInterface temp = (GridSchedulerNodeInterface) registry.lookup(bro_url);
						temp.onMessageReceived(controlMessage);
					} catch (RemoteException e) {
						//e.printStackTrace();
					} catch (NotBoundException e) {
						//e.printStackTrace();
					} catch (InterruptedException e) {
						//e.printStackTrace();
					} 
					catch (Exception e){
						
					}
				}
			}
		}
		//
		if(controlMessage.getType() == ControlMessageType.NodeStart){
			//send all jobs from log that has the name of the crashed node
			// loop over all resource managers, and pick the one with the lowest load
			heartbeat_hashmap.put(controlMessage.getUrl(), new Date());
			Registry registry;
			for (long key : job_log.keySet())
			{
				if(job_log.get(key)!=null){
					if(job_log.get(key).equals(controlMessage.getUrl())){
						try{
//							System.setProperty("java.rmi.server.hostname", adress.get(controlMessage.getUrl())); 
							registry = LocateRegistry.getRegistry(adress.get(controlMessage.getUrl()),port.get(controlMessage.getUrl()));
							ControlMessage cMessage = new ControlMessage(ControlMessageType.AddJob);
							cMessage.setJob(job_log2.get(key));
							cMessage.setUrl(url);
							cMessage.setAdress(adrr);
							cMessage.setPort(portnr);
							if(!controlMessage.fromCluster()){
								GridSchedulerNodeInterface temp = (GridSchedulerNodeInterface) registry.lookup(controlMessage.getUrl());
								temp.onMessageReceived(cMessage);
							}else if(controlMessage.fromCluster()){
								ResourceManagerInterface temp = (ResourceManagerInterface) registry.lookup(controlMessage.getUrl());
								temp.onMessageReceived(cMessage);
							}
							
							
						}catch (RemoteException e) {
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
			}
				
		}
		//
		if(controlMessage.getType() == ControlMessageType.HeartBeat){
			ControlMessage cMessage = new ControlMessage(ControlMessageType.HeartBeatReply);
			cMessage.setUrl(this.getUrl());
			cMessage.setAdress(adrr);
			cMessage.setPort(portnr);
			cMessage.setFromCluster(false);
			Registry registry;
			try {
//				System.setProperty("java.rmi.server.hostname", adress.get(rmUrl)); 
				registry = LocateRegistry.getRegistry(adress.get(controlMessage.getUrl()),port.get(controlMessage.getUrl()));
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
		//
		if(controlMessage.getType() == ControlMessageType.HeartBeatReply){
			heartbeat_hashmap.put(controlMessage.getUrl(),new Date());
		}
		//
		if(controlMessage.getType() == ControlMessageType.Election){
			//if election criteria true, the node that sent this should win
				//send electionreply
			//if i should 
			if(controlMessage.getUrl().compareTo(url) > 0){
				//send reply
				ControlMessage cMessage = new ControlMessage(ControlMessageType.ElectionReply);
				cMessage.setUrl(this.getUrl());
				cMessage.setAdress(adrr);
				cMessage.setPort(portnr);
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
			else{
				//broadcast election message? nah
			}
		}
		//
		if(controlMessage.getType() == ControlMessageType.ElectionReply){
			if(elect.get(controlMessage.getJob().getId()) == null){
				elect.put(controlMessage.getJob().getId(), 1);
			}else{
				elect.put(controlMessage.getJob().getId(), elect.get(controlMessage.getJob().getId())+1);
			}
			if(elect.get(controlMessage.getJob().getId()) == gridScheduler.size()-1){
				controlMessage.getJob().addToLog(url);
				controlMessage.getJob().setLast(url);
				jobQueue.add(controlMessage.getJob());
				System.out.println("reclaimed job nr."+ controlMessage.getJob().getId() + "from crashed node");
				
				//check if job is in message log
				job_log.put(controlMessage.getJob().getId(), controlMessage.getUrl());
				job_log2.put(controlMessage.getJob().getId(), controlMessage.getJob());
				
				//jobArrived broadcast
				for (String bro_url : gridScheduler)
				{
					ControlMessage cMessage = new ControlMessage(ControlMessageType.JobArrival);
					cMessage.setUrl(this.getUrl());
					cMessage.setAdress(this.adrr);
					cMessage.setPort(portnr);
					cMessage.setJob(controlMessage.getJob());
					Registry registry;
					try {
						registry = LocateRegistry.getRegistry(adress.get(bro_url),port.get(bro_url));
						GridSchedulerNodeInterface temp = (GridSchedulerNodeInterface) registry.lookup(bro_url);
						temp.onMessageReceived(cMessage);
					} catch (RemoteException e) {
						////e.printStackTrace();
					} catch (NotBoundException e) {
						//e.printStackTrace();
					} catch (InterruptedException e) {
						//e.printStackTrace();
					} 
					catch(Exception e){
						
					}
				}
			}
		}
		//
		if(controlMessage.getType() == ControlMessageType.CrashNotification){
			append(url+".txt", controlMessage.getUrl() + "- "+ controlMessage.getTimestamp().toString()+ ": " + controlMessage.getSubject() + " has crashed");
		}
		
//		if(Math.random() * 1000 <= 1){
//			System.out.println(url + ": crashed" );
//			NodeCrash();
//			return;
//		}
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
			for (String rmUrl : resourceManagerLoad.keySet())
			{
				ControlMessage cMessage = new ControlMessage(ControlMessageType.HeartBeat);
				cMessage.setUrl(this.getUrl());
				cMessage.setAdress(adrr);
				cMessage.setPort(portnr);
				Registry registry;
				try {
					registry = LocateRegistry.getRegistry(adress.get(rmUrl),port.get(rmUrl));
					ResourceManagerInterface temp = (ResourceManagerInterface) registry.lookup(rmUrl);
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
			for (String rmUrl : NodeLoad.keySet())
			{
				ControlMessage cMessage = new ControlMessage(ControlMessageType.HeartBeat);
				cMessage.setUrl(this.getUrl());
				cMessage.setAdress(adrr);
				cMessage.setPort(portnr);
				Registry registry;
				try {
					registry = LocateRegistry.getRegistry(adress.get(rmUrl),port.get(rmUrl));
					GridSchedulerNodeInterface temp = (GridSchedulerNodeInterface) registry.lookup(rmUrl);
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
			
			for (String rmUrl : resourceManagerLoad.keySet())
			{
				if(new Date().getTime() - heartbeat_hashmap.get(rmUrl).getTime() > 5000 && resourceManagerLoad.get(rmUrl) != Integer.MAX_VALUE){
					resourceManagerLoad.put(rmUrl, Integer.MAX_VALUE);
					System.out.println(rmUrl + " is down");
					node_life.put(rmUrl, false);
					append(url+".txt", url + "- "+ new Date().toString()+ ": " + rmUrl + " has crashed");
//					set load to max
//					set jobs of that rm, in my queue
					for (long key : job_log.keySet())
					{
						if(job_log.get(key)!=null){
							if(job_log.get(key).equals(rmUrl)){
								
								
								job_log2.get(key).addToLog(url);
								job_log2.get(key).setLast(url);
								
								//check if job is in message log
								job_log.put(key, url);
								
								jobQueue.add(job_log2.get(key));
								//add to log TODO
//								some_log.put(controlMessage.getJob().toString(), new Date());
								
								//TODO notify nodes that resourcemanager crashed
								
								//jobArrived broadcast
								for (String bro_url : gridScheduler)
								{
									ControlMessage cMessage = new ControlMessage(ControlMessageType.JobArrival);
									cMessage.setUrl(this.getUrl());
									cMessage.setAdress(this.adrr);
									cMessage.setPort(portnr);
									cMessage.setJob(job_log2.get(key));
									Registry registry;
									try {
										registry = LocateRegistry.getRegistry(adress.get(bro_url),port.get(bro_url));
										GridSchedulerNodeInterface temp = (GridSchedulerNodeInterface) registry.lookup(bro_url);
										temp.onMessageReceived(cMessage);
									} catch (RemoteException e) {
										////e.printStackTrace();
									} catch (NotBoundException e) {
										//e.printStackTrace();
									} catch (InterruptedException e) {
										//e.printStackTrace();
									} 
									catch(Exception e){
										
									}
								}
							}
						}
					}
				}else{
					ControlMessage cMessage = new ControlMessage(ControlMessageType.RequestLoad);
					cMessage.setUrl(this.getUrl());
					cMessage.setAdress(adrr);
					cMessage.setPort(portnr);
					Registry registry;
					try {
	//					System.setProperty("java.rmi.server.hostname", adress.get(rmUrl)); 
						registry = LocateRegistry.getRegistry(adress.get(rmUrl),port.get(rmUrl));
						ResourceManagerInterface temp = (ResourceManagerInterface) registry.lookup(rmUrl);
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
			for (String rmUrl : NodeLoad.keySet())
			{
				if(new Date().getTime() - heartbeat_hashmap.get(rmUrl).getTime() > 5000 && NodeLoad.get(rmUrl) != Integer.MAX_VALUE){
					NodeLoad.put(rmUrl, Integer.MAX_VALUE);
					System.out.println(rmUrl + " is down");
					active_nodes.getAndDecrement();
					node_life.put(rmUrl, false);
	//				set load to max
	//				set jobs of that rm, in my queue
					//TODO notify nodes that a node crashed
					Date timestamp = new Date();
					//jobArrived broadcast
					for (String bro_url : gridScheduler)
					{
						if(!bro_url.equals(rmUrl)){
							ControlMessage cMessage = new ControlMessage(ControlMessageType.CrashNotification);
							cMessage.setUrl(this.getUrl());
							cMessage.setAdress(this.adrr);
							cMessage.setPort(portnr);
							cMessage.setTimestamp(timestamp);
							cMessage.setSubject(rmUrl);
							Registry registry;
							try {
								registry = LocateRegistry.getRegistry(adress.get(bro_url),port.get(bro_url));
								GridSchedulerNodeInterface temp = (GridSchedulerNodeInterface) registry.lookup(bro_url);
								temp.onMessageReceived(cMessage);
							} catch (RemoteException e) {
								////e.printStackTrace();
							} catch (NotBoundException e) {
								//e.printStackTrace();
							} catch (InterruptedException e) {
								//e.printStackTrace();
							} 
							catch(Exception e){
								
							}
						}
					}
					
					
					if(NodeLoad.keySet().size() == 1){
						for (long key : job_log.keySet())
						{
							if(job_log.get(key)!=null){
								if(job_log.get(key).equals(rmUrl)){
									System.out.println("salvaged job");
									job_log2.get(rmUrl).addToLog(url);
									job_log2.get(rmUrl).setLast(url);
									job_log.put(key, url);
									jobQueue.add(job_log2.get(rmUrl));
								}
							}
						}
					}
					else{
						for (String inception : NodeLoad.keySet())
							{
								if(inception!=rmUrl){
									for (long key : job_log.keySet())
									{
										if(job_log.get(key)!=null){
											if(job_log.get(key).equals(rmUrl)){
												ControlMessage cMessage = new ControlMessage(ControlMessageType.Election);
												cMessage.setUrl(this.getUrl());
												cMessage.setAdress(adrr);
												cMessage.setPort(portnr);
												cMessage.setJob(job_log2.get(key));
												Registry registry;
												try {
													registry = LocateRegistry.getRegistry(adress.get(inception),port.get(inception));
													GridSchedulerNodeInterface temp = (GridSchedulerNodeInterface) registry.lookup(inception);
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
									}
								}
							}
					}
				}else{
					ControlMessage cMessage = new ControlMessage(ControlMessageType.RequestLoad);
					cMessage.setUrl(this.getUrl());
					cMessage.setAdress(adrr);
					cMessage.setPort(portnr);
					Registry registry;
					try {
	//					System.setProperty("java.rmi.server.hostname", adress.get(rmUrl)); 
						registry = LocateRegistry.getRegistry(adress.get(rmUrl),port.get(rmUrl));
						GridSchedulerNodeInterface temp = (GridSchedulerNodeInterface) registry.lookup(rmUrl);
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
			
			// schedule waiting messages to the different clusters
			for (Job job : jobQueue)
			{
				String leastLoadedRM =  getLeastLoadedRM();
				String leastLoadedNode =  getLeastLoadedNode();
				Registry registry;
				if (leastLoadedRM!=null) {
					
					
					if( (resourceManagerLoad.get(leastLoadedRM) < NodeLoad.get(leastLoadedNode)) ){
						ControlMessage cMessage = new ControlMessage(ControlMessageType.AddJob);
						cMessage.setJob(job);
						cMessage.setUrl(url);
						cMessage.setAdress(adrr);
						cMessage.setPort(portnr);
						try {
//							System.setProperty("java.rmi.server.hostname", adress.get(leastLoadedRM)); 
							registry = LocateRegistry.getRegistry(adress.get(leastLoadedRM),port.get(leastLoadedRM));
							ResourceManagerInterface temp = (ResourceManagerInterface) registry.lookup(leastLoadedRM);
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
						jobQueue.remove(job);
						
						// increase the estimated load of that RM by 1 (because we just added a job)
						int load = resourceManagerLoad.get(leastLoadedRM);
						resourceManagerLoad.put(leastLoadedRM, load+1);
						
						job_log.put(job.getId(), leastLoadedRM);
						job_log2.put(job.getId(), job);
					}
					else{
						try {
//							System.setProperty("java.rmi.server.hostname", adress.get(leastLoadedNode)); 
							registry = LocateRegistry.getRegistry(adress.get(leastLoadedNode),port.get(leastLoadedNode));
							if (leastLoadedNode!=null) {
								ControlMessage cMessage = new ControlMessage(ControlMessageType.AddJob);
								job.setNodeToNode();
								cMessage.setJob(job);
								cMessage.setUrl(url);
								
								GridSchedulerNodeInterface temp = (GridSchedulerNodeInterface) registry.lookup(leastLoadedNode);
								temp.onMessageReceived(cMessage);
								
//								jobQueue.remove(job);
								
								jobQueue.remove(job);
								if( NodeLoad.get(leastLoadedNode)!=null){
									int load = NodeLoad.get(leastLoadedNode);
									NodeLoad.put(leastLoadedNode, load+1);
								}
								job_log.put(job.getId(), leastLoadedNode);
								job_log2.put(job.getId(), job);
							}
							
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
			}
			
			
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
//		this.resourceManagerLoad = new ConcurrentHashMap<String, Integer>();
//		this.NodeLoad = new ConcurrentHashMap<String, Integer>();
		this.jobQueue = new ConcurrentLinkedQueue<Job>();
//		job_log = new ConcurrentHashMap<Long, String>();
//		job_log2 = new ConcurrentHashMap<Long, Job>();
		
		Thread.sleep(60000);
		
		for (String rmUrl : resourceManagerLoad.keySet())
		{
			ControlMessage cMessage = new ControlMessage(ControlMessageType.RequestLoad);
			cMessage.setUrl(this.getUrl());
			Registry registry;
			try {
//				System.setProperty("java.rmi.server.hostname", adress.get(rmUrl)); 
				registry = LocateRegistry.getRegistry(adress.get(rmUrl),port.get(rmUrl));
				ResourceManagerInterface temp = (ResourceManagerInterface) registry.lookup(rmUrl);
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
		for (String rmUrl : NodeLoad.keySet())
		{
			ControlMessage cMessage = new ControlMessage(ControlMessageType.RequestLoad);
			cMessage.setUrl(this.getUrl());
			Registry registry;
			try {
//				System.setProperty("java.rmi.server.hostname", adress.get(rmUrl)); 
				registry = LocateRegistry.getRegistry(adress.get(rmUrl),port.get(rmUrl));
				GridSchedulerNodeInterface temp = (GridSchedulerNodeInterface) registry.lookup(rmUrl);
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
		
		//for each gsn, send msg
		//they send addjob messages?
		// send a message to each resource manager, requesting its load
		running = true;
		this.run();
		ControlMessage cMessage = new ControlMessage(ControlMessageType.NodeStart);
		cMessage.setUrl(url);
		cMessage.setFromCluster(false);
		for (String bro_url : gridScheduler)
		{
			Registry registry;
			try {
//				System.setProperty("java.rmi.server.hostname", adress.get(bro_url)); 
				registry = LocateRegistry.getRegistry(adress.get(bro_url),port.get(bro_url));
				GridSchedulerNodeInterface temp = (GridSchedulerNodeInterface) registry.lookup(bro_url);
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
		
		//record restart time
		//record recovery time
	}
	
	public void connectToGridScheduler(String gridSchedulerURL, String adress, int port) throws RemoteException, NotBoundException, InterruptedException {

		// preconditions
		assert(gridSchedulerURL != null) : "the parameter 'gridSchedulerURL' cannot be null"; 

		gridScheduler.add(gridSchedulerURL);
		this.adress.put(gridSchedulerURL, adress);
		this.port.put(gridSchedulerURL, port);
		NodeLoad.put(gridSchedulerURL, Integer.MAX_VALUE);
		heartbeat_hashmap.put(gridSchedulerURL, new Date());
		ControlMessage message = new ControlMessage(ControlMessageType.GridSchedulerNodeJoin);
		message.setUrl(url);
		message.setAdress(this.adrr);
		message.setPort(portnr);
		
		Registry registry = LocateRegistry.getRegistry(adress,port);
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
	
//	public String toString(){
//		String result = "";
////		for (String key : completion.keySet())
////		{
////			result += "["+ key + ": " + completion.get(key) + "]";
////		}
//		return url + ": " + comp.size();
//	}
	
	
	public static void append(String file, String in){
		FileWriter fw = null;
		BufferedWriter bw = null;
		PrintWriter out = null;
		try {
		    fw = new FileWriter(file, true);
		    bw = new BufferedWriter(fw);
		    out = new PrintWriter(bw);
		    out.println(in);
		    out.close();
		} catch (IOException e) {
		    //exception handling left as an exercise for the reader
		}
		finally {
		    if(out != null)
			    out.close();
		    try {
		        if(bw != null)
		            bw.close();
		    } catch (IOException e) {
		        //exception handling left as an exercise for the reader
		    }
		    try {
		        if(fw != null)
		            fw.close();
		    } catch (IOException e) {
		        //exception handling left as an exercise for the reader
		    }
		}
	}
}
