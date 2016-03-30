package distributed.systems.gridscheduler.model;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface GridSchedulerNodeInterface extends Remote{

	public void onMessageReceived(ControlMessage message) throws RemoteException;
}
