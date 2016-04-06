package distributed.systems.gridscheduler.model;

import java.io.Serializable;
import java.util.Date;

/**
 * This class represents a job that can be executed on a grid. 
 * 
 * @author Niels Brouwers edited by Carlo van der Valk
 *
 */
public class Job  implements Serializable{
	private long duration;
	private JobStatus status;
	private long id;
	private String log;
	private String last;
	private boolean nodeToNode;
	private Date timeArrived;
	private Date timeCompleted;

	/**
	 * Constructs a new Job object with a certain duration and id. The id has to be unique
	 * within the distributed system to avoid collisions.
	 * <P>
	 * <DL>
	 * <DT><B>Preconditions:</B>
	 * <DD>parameter <CODE>duration</CODE> should be positive
	 * </DL> 
	 * @param duration job duration in milliseconds 
	 * @param id job ID
	 */
	public Job(long duration, long id) {
		// Preconditions
		assert(duration > 0) : "parameter 'duration' should be > 0";

		this.duration = duration;
		this.status = JobStatus.Waiting;
		this.id = id; 
		log = "";
		last = "";
		nodeToNode = false;
	}

	/**
	 * Returns the duration of this job. 
	 * @return the total duration of this job
	 */
	public double getDuration() {
		return duration;
	}

	/**
	 * Returns the status of this job.
	 * @return the status of this job
	 */
	public JobStatus getStatus() {
		return status;
	}

	/**
	 * Sets the status of this job.
	 * @param status the new status of this job
	 */
	public void setStatus(JobStatus status) {
		this.status = status;
	}

	/**
	 * The message ID is a unique identifier for a message. 
	 * @return the message ID
	 */
	public long getId() {
		return id;
	}

	/**
	 * @return a string representation of this job object
	 */
	public String toString() {
		long wait = ((( timeCompleted.getTime() - timeArrived.getTime() ) - duration));
		return "Job {ID = " + id + ", " + "wait=" + wait + ", "+ log + "}";
	}
	
	public void addToLog(String clusterOrNode){
		log += ", "+clusterOrNode;
	}
	
	public String getLog(){
		return log;
	}
	
	public void setLog(String log){
		this.log = log;
	}
	
	public void setLast(String last){
		this.last = last;
	}
	
	public String getLast(){
		return last;
	}
	
	public void setNodeToNode(){
		nodeToNode = true;
	}
	
	public boolean isNodeToNode(){
		return nodeToNode;
	}
	
	public void setTimeArrived(){
		timeArrived = new Date();
	}
	
	public Date getTimeArrived(){
		return timeArrived;
	}
	
	public void setTimeCompleted(){
		timeCompleted = new Date();
	}
	
	public Date getTimeCompleted(){
		return timeCompleted;
	}

}
