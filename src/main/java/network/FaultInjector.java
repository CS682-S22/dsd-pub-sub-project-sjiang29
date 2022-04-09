package network;

public interface FaultInjector {
    public boolean shouldFail();
    public void injectFailure();
}
