package service;

import java.util.Hashtable;

public class FailureDetector {
    private Hashtable<Integer, Long> heartBeatReceivedTimes;
    private long timeoutNanos;
    private HeartBeatScheduler heartbeatScheduler;

    public FailureDetector(Hashtable<Integer, Long> heartBeatReceivedTimes, long timeoutNanos) {
        this.heartBeatReceivedTimes = heartBeatReceivedTimes;
        this.timeoutNanos = timeoutNanos;
        this.heartbeatScheduler = new HeartBeatScheduler(new HeartBeatChecker(heartBeatReceivedTimes, timeoutNanos ), 100);;
    }

    public void start(){
        this.heartbeatScheduler.start();
    }
}
