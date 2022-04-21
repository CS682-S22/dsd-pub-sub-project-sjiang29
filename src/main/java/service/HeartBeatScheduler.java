package service;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;


/*Reference from https://martinfowler.com/articles/patterns-of-distributed-systems/heartbeat.html **/
/**
 * HeartBeatScheduler: class to do heart beat or heart beat check periodically
 */
public class HeartBeatScheduler {
    private ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1);
    private Runnable action;
    private int heartBeatInterval;

    /**
     * Constructor
     * @param action
     * @param heartBeatInterval
     */
    public HeartBeatScheduler(Runnable action, int heartBeatInterval){
        this.action = action;
        this.heartBeatInterval = heartBeatInterval;
    }

    private ScheduledFuture<?> scheduledTask;

    /**
     * Method to start scheduled task
     */
    public void start(){
        scheduledTask = executor.scheduleWithFixedDelay(this.action, this.heartBeatInterval, this.heartBeatInterval, TimeUnit.MILLISECONDS);
    }
}
