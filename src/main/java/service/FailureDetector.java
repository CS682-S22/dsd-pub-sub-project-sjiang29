package service;

import network.Connection;

import java.util.Hashtable;

public class FailureDetector {
    private String hostBrokerName;
    private Hashtable<Integer, Long> heartBeatReceivedTimes;
    private long timeoutNanos;
    private HeartBeatScheduler heartbeatScheduler;
    private Membership membership;
    Hashtable<Integer, Connection> connections;

    public FailureDetector(String hostBrokerName, Hashtable<Integer, Long> heartBeatReceivedTimes, long timeoutNanos,
                           Membership membership, Hashtable<Integer, Connection> connections) {
        this.hostBrokerName = hostBrokerName;
        this.heartBeatReceivedTimes = heartBeatReceivedTimes;
        this.timeoutNanos = timeoutNanos;
        this.connections = connections;
        this.heartbeatScheduler = new HeartBeatScheduler(new HeartBeatChecker(hostBrokerName, heartBeatReceivedTimes, timeoutNanos, membership, connections ), 100);;
    }

    public void start(){
        this.heartbeatScheduler.start();
    }
}
