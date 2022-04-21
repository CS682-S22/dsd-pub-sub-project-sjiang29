package network;

/**
 * network.FaultInjector interface: can receive message
 */
public interface FaultInjector {
    /**
     * Interface method
     * @return
     */
    public boolean shouldFail();

    /**
     * Interface method
     * @return
     */
    public void injectFailure();
}
