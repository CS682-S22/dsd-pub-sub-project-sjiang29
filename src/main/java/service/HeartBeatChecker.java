package service;

import com.google.protobuf.ByteString;
import network.Connection;
import proto.MsgInfo;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Set;
import static framework.Broker.logger;
import static framework.Broker.isElecting;


public class HeartBeatChecker implements Runnable{
    private String hostBrokerName;
    private Hashtable<Integer, Long> heartBeatReceivedTimes;
    private long timeoutNanos;
    private Membership membership;
    private Hashtable<Integer, Connection> connections;

    public HeartBeatChecker(String hostBrokerName, Hashtable<Integer, Long> heartBeatReceivedTimes, long timeoutNanos, Membership membership,
                            Hashtable<Integer, Connection> connections) {
        this.hostBrokerName = hostBrokerName;
        this.heartBeatReceivedTimes = heartBeatReceivedTimes;
        this.timeoutNanos = timeoutNanos;
        this.membership = membership;
        this.connections = connections;
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
                if(id == leaderId){
                    sendBullyReq();
                }
            }
        }
    }

    private void sendBullyReq(){
        boolean hasLargerId = false;
        ArrayList<Integer> liveMembersId = this.membership.getLiveMembers();
        int hostBrokerId = this.membership.getId(this.hostBrokerName);
        MsgInfo.Msg electionMsg = MsgInfo.Msg.newBuilder().setType("election").setSenderName(this.hostBrokerName).build();
        for(int i : liveMembersId){
            if(i > hostBrokerId) {
                hasLargerId = true;
                Connection connection = connections.get(i);
                connection.send(electionMsg.toByteArray());
            }
        }
        if(!hasLargerId){
            MsgInfo.Msg coordinatorMsg = MsgInfo.Msg.newBuilder().setType("coordinator").setSenderName(this.hostBrokerName).build();
            for(int i : liveMembersId){
                Connection connection = connections.get(i);
                connection.send(coordinatorMsg.toByteArray());
            }

        }

    }
}
