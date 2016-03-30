package distributed.systems.gridscheduler.model;

import java.rmi.NotBoundException;
import java.rmi.Remote;
import java.rmi.RemoteException;

public interface ResourceManagerInterface extends Remote{
	
	public void onMessageReceived(ControlMessage message) throws RemoteException, NotBoundException;

}
