package distributed.systems.gridscheduler.model;

/**
 * 
 * Event handler for nodes. This allows nodes to communicate their status back to the cluster
 * they are in.
 * 
 * @author Niels Brouwers edited by Carlo van der Valk
 *
 */
public interface INodeEventHandler {

	// notify the completion of a job
	public void jobDone(Job job);

}
