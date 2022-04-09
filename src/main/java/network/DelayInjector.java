package network;

public class DelayInjector implements FaultInjector{

    private int delay;

    public DelayInjector(int delay) {
        this.delay = delay;
    }

    @Override
    public boolean shouldFail() {
        return false;
    }

    @Override
    public void injectFailure() {
        try {
            Thread.sleep(this.delay);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }
}
