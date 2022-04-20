package service;

import com.google.protobuf.ByteString;
import framework.Broker;
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
    private ConcurrentHashMap<String, Connection> brokerConnections;
    private Connection connectionToLoadBalancer;


    public HeartBeatChecker(String hostBrokerName, Hashtable<Integer, Long> heartBeatReceivedTimes, long timeoutNanos, Membership membership,
                            ConcurrentHashMap<String, Connection> brokerConnections, Connection connectionToLoadBalancer) {
        this.hostBrokerName = hostBrokerName;
        this.heartBeatReceivedTimes = heartBeatReceivedTimes;
        this.timeoutNanos = timeoutNanos;
        this.membership = membership;
        this.brokerConnections = brokerConnections;
        this.connectionToLoadBalancer = connectionToLoadBalancer;
    }

    @Override
    public void run() {
        Long now = System.nanoTime();
        Set<Integer> brokerIds = this.membership.getAllMembers();

        int leaderId = this.membership.getLeaderId();
        logger.info("hb checker line 40:leaderId " + leaderId);
        for (Integer id : brokerIds) {
            Long lastHeartbeatReceivedTime = this.heartBeatReceivedTimes.get(id);
            Long timeSinceLastHeartbeat = now - lastHeartbeatReceivedTime;
            if (timeSinceLastHeartbeat >= timeoutNanos) {
                logger.info("hb checker line 45: mark down: " + id);
                this.membership.markDown(id);
                this.membership.printLiveMembers();
                // leader is down
                if (id == leaderId) {
                    logger.info("hb checker line 49: start bully" );
                    Broker.isElecting = true;
                    int newLeaderId = BullyAlgo.sendBullyReq(this.membership, this.hostBrokerName, this.brokerConnections, this.connectionToLoadBalancer);
                    if (newLeaderId != -1) {
                        //Broker.isElecting = false;
                        this.membership.setLeaderId(newLeaderId);
                        //logger.info("bully algo line 44: send coordinator msg to load balancer from" + newLeaderId);
                        //connectionToLoadBalancer.send(coordinatorMsg.toByteArray());
                    }
                }
            }
        }
    }
}


