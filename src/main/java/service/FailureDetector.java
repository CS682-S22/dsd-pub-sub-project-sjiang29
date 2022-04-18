package service;

import network.Connection;

import java.util.Hashtable;
import java.util.concurrent.ConcurrentHashMap;

public class FailureDetector {
    private String hostBrokerName;
    private Hashtable<Integer, Long> heartBeatReceivedTimes;
    private long timeoutNanos;
    private HeartBeatScheduler heartbeatScheduler;
    private Membership membership;
    private ConcurrentHashMap<String, Connection> connections;
    private Connection connectionToLoadBalancer;

    public FailureDetector(String hostBrokerName, Hashtable<Integer, Long> heartBeatReceivedTimes, long timeoutNanos,
                           Membership membership, ConcurrentHashMap<String, Connection> connections, Connection connectionToLoadBalancer) {
        this.hostBrokerName = hostBrokerName;
        this.heartBeatReceivedTimes = heartBeatReceivedTimes;
        this.timeoutNanos = timeoutNanos;
        this.connections = connections;
        this.heartbeatScheduler = new HeartBeatScheduler(new HeartBeatChecker(hostBrokerName, heartBeatReceivedTimes, timeoutNanos, membership, connections, connectionToLoadBalancer ), 100);;
    }

    public void start(){
        this.heartbeatScheduler.start();
    }
}
