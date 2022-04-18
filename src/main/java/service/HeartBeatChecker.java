package service;

import com.google.protobuf.ByteString;
import network.Connection;
import proto.MsgInfo;
import utils.Config;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static framework.Broker.logger;



public class HeartBeatChecker implements Runnable {
    private String hostBrokerName;
    private Hashtable<Integer, Long> heartBeatReceivedTimes;
    private long timeoutNanos;
    private Membership membership;
    private ConcurrentHashMap<String, Connection> connections;
    private Connection connectionToLoadBalancer;


    public HeartBeatChecker(String hostBrokerName, Hashtable<Integer, Long> heartBeatReceivedTimes, long timeoutNanos, Membership membership,
                            ConcurrentHashMap<String, Connection> connections, Connection connectionToLoadBalancer) {
        this.hostBrokerName = hostBrokerName;
        this.heartBeatReceivedTimes = heartBeatReceivedTimes;
        this.timeoutNanos = timeoutNanos;
        this.membership = membership;
        this.connections = connections;
        this.connectionToLoadBalancer = connectionToLoadBalancer;
    }

    @Override
    public void run() {
        Long now = System.nanoTime();
        Set<Integer> brokerIds = this.heartBeatReceivedTimes.keySet();
        int leaderId = this.membership.getLeaderId();
        for (Integer id : brokerIds) {
            Long lastHeartbeatReceivedTime = this.heartBeatReceivedTimes.get(id);
            Long timeSinceLastHeartbeat = now - lastHeartbeatReceivedTime;
            if (timeSinceLastHeartbeat >= timeoutNanos) {
                this.membership.markDown(id);
                // leader is down
                if (id == leaderId) {
                    logger.info("hb checker line 45: start bully" );
                    int newLeaderId = BullyAlgo.sendBullyReq(this.membership, this.hostBrokerName, this.connections, this.connectionToLoadBalancer);
                    if (newLeaderId != -1) {
                        this.membership.setLeaderId(newLeaderId);
                    }
                }
            }
        }
    }
}


