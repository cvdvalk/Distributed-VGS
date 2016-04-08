package distributed.systems.gridscheduler.model;

/**
 * 
 * Different types of control messages. Feel free to add new message types if you need any. 
 * 
 * @author Niels Brouwers edited by Carlo van der Valk
 *
 */
public enum ControlMessageType {

	// from RM to GS
	ResourceManagerJoin,
	GridSchedulerNodeJoin,
	ReplyLoad,

	// from GS to RM
	RequestLoad,

	// both ways
	AddJob,
	
	//from GSN to GSN
	JobArrival, 		// from RM to GS, and GS to GS?
	JobStart,			// from RM to GS
	JobCompletion,		// from RM to GS
	NodeStart,			//from GSN to GSN
	NodeStartReply, 	//from GSN to GSN
	ReplyToNode,
	HeartBeat,
	HeartBeatReply,
	Election,
	ElectionReply,
	CrashNotification
}
