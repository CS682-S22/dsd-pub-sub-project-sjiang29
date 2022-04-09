package service;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;


/*Reference from https://martinfowler.com/articles/patterns-of-distributed-systems/heartbeat.html **/
public class HeartBeatScheduler {
    private ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1);
    private Runnable action;
    private int heartBeatInterval;

    public HeartBeatScheduler(Runnable action, int heartBeatInterval){
        this.action = action;
        this.heartBeatInterval = heartBeatInterval;
    }

    private ScheduledFuture<?> scheduledTask;

    public void start(){
        scheduledTask = executor.scheduleWithFixedDelay(this.action, this.heartBeatInterval, this.heartBeatInterval, TimeUnit.MILLISECONDS);
    }
}
