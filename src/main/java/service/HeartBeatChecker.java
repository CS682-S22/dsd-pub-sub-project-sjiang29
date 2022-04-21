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
import java.util.concurrent.CopyOnWriteArrayList;

import static framework.Broker.logger;



public class HeartBeatChecker implements Runnable {
    private String hostBrokerName;
    private ConcurrentHashMap<Integer, Long> heartBeatReceivedTimes;
    private long timeoutNanos;
    private Membership membership;
    private ConcurrentHashMap<String, Connection> brokerConnections;
    private Connection connectionToLoadBalancer;
    private ConcurrentHashMap<String, CopyOnWriteArrayList<MsgInfo.Msg>> msgLists;


    public HeartBeatChecker(String hostBrokerName,
                            ConcurrentHashMap<Integer, Long> heartBeatReceivedTimes,
                            long timeoutNanos, Membership membership,
                            ConcurrentHashMap<String, Connection> brokerConnections,
                            Connection connectionToLoadBalancer,
                            ConcurrentHashMap<String, CopyOnWriteArrayList<MsgInfo.Msg>> msgLists) {
        this.hostBrokerName = hostBrokerName;
        this.heartBeatReceivedTimes = heartBeatReceivedTimes;
        this.timeoutNanos = timeoutNanos;
        this.membership = membership;
        this.brokerConnections = brokerConnections;
        this.connectionToLoadBalancer = connectionToLoadBalancer;
        this.msgLists = msgLists;
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
                    int newLeaderId = BullyAlgo.sendBullyReq(this.membership, this.hostBrokerName, this.brokerConnections,
                            this.connectionToLoadBalancer, this.msgLists);
                    if (newLeaderId != -1) {
                        Broker.isElecting = false;
                        this.membership.setLeaderId(newLeaderId);
                        //logger.info("bully algo line 44: send coordinator msg to load balancer from" + newLeaderId);
                        //connectionToLoadBalancer.send(coordinatorMsg.toByteArray());
                    }
                }
            }
        }
    }
}


