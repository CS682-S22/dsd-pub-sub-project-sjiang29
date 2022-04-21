package network;


/**
 * DelayInjector class which implements FaultInjector interface
 */
public class DelayInjector implements FaultInjector{

    private int delay;

    /**
     * DelayInjector constructor
     */
    public DelayInjector(int delay) {
        this.delay = delay;
    }

    /**
     * Interface method
     * @return
     */
    @Override
    public boolean shouldFail() {
        return false;
    }

    /**
     * Interface method
     * @return
     */
    @Override
    public void injectFailure() {
        try {
            Thread.sleep(this.delay);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }
}
