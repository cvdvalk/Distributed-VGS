package distributed.systems.gridscheduler.model;

import java.io.Serializable;
import java.util.Date;



/**
 * 
 * Class that represents the messages being exchanged in the VGS. It has some members to
 * facilitate the passing of common arguments. Feel free to expand it and adapt it to your 
 * needs. 
 * 
 * @author Niels Brouwers edited by Carlo van der Valk
 *
 */
public class ControlMessage implements Serializable{

	/**
	 * Generated serial version UID
	 */
	private static final long serialVersionUID = -1453428681740343634L;

	private final ControlMessageType type;
	private String url;
	private Job job;
	private int load;
	private int max;
	private boolean fromCluster;
	private String adress;
	private int port;
	private String elect;
	private Date timestamp;
	private String subject;
	
	/**
	 * Constructs a new ControlMessage object
	 * @param type the type of control message
	 */
	public ControlMessage(ControlMessageType type) {
		this.type = type;
	}

	/**
	 * @return the job
	 */
	public Job getJob() {
		return job;
	}

	/**
	 * <DL>
	 * <DT><B>Preconditions:</B>
	 * <DD>parameter <CODE>job</CODE> cannot be null
	 * </DL>
	 * @param job the job to set
	 */
	public void setJob(Job job) {
		assert(job != null) : "parameter 'job' cannot be null";
		this.job = job;
	}

	/**
	 * @return the load
	 */
	public int getLoad() {
		return load;
	}

	/**
	 * @param load the load to set
	 */
	public void setLoad(int load) {
		this.load = load;
	}

	/**
	 * @return the url
	 */
	public String getUrl() {
		return url;
	}

	/**
	 * @param url the url to set
	 */
	public void setUrl(String url) {
		this.url = url;
	}

	/**
	 * @return the type
	 */
	public ControlMessageType getType() {
		return type;
	}
	
	public void setMax(int max){
		this.max = max;
	}
	
	public int getMax(){
		return max;
	}
	
	public void setFromCluster(boolean thing){
		fromCluster = thing;
	}
	
	public boolean fromCluster(){
		return fromCluster;
	}
	
	public void setAdress(String adress){
		this.adress=adress;
	}
	
	public String getAdress(){
		return adress;
	}
	
	public void setPort(int port){
		this.port = port;
	}
	
	public int getPort(){
		return port;
	}

	public void setElect(String elect){
		this.elect = elect;
	}
	
	public String getElect(){
		return elect;
	}
	
	public void setTimestamp(Date timestamp){
		this.timestamp = timestamp;
	}
	
	public Date getTimestamp(){
		return timestamp;
	}
	
	public void setSubject(String subject){
		this.subject = subject;
	}
	
	public String getSubject(){
		return this.subject;
	}

}
