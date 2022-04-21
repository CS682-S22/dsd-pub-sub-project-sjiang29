package service;

import framework.Broker;
import framework.Server;
import network.Connection;
import proto.MsgInfo;
import utils.Config;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import static framework.Broker.logger;


/**
 * BullyAlgo: class to implement bully algorithm
 */
public class BullyAlgo {


    /**
     * static method to implement bully algorithm
     * @param brokerConnections
     * @param connectionToLoadBalancer
     * @param hostBrokerName
     * @param membership
     * @param msgLists
     */
    public static int sendBullyReq(Membership membership, String hostBrokerName,
                                   ConcurrentHashMap<String, Connection> brokerConnections,
                                   Connection connectionToLoadBalancer,
                                   ConcurrentHashMap<String, CopyOnWriteArrayList<MsgInfo.Msg>> msgLists){

        Broker.isElecting = true;
        int newLeaderId = -1;
        boolean hasLargerId = false;
        Set<Integer> liveMembersId = membership.getAllMembers();
        int hostBrokerId = Config.nameToId.get(hostBrokerName);
        MsgInfo.Msg electionMsg = MsgInfo.Msg.newBuilder().setType("election").setSenderName(hostBrokerName).build();
        for(int i : liveMembersId){
            if(i > hostBrokerId) {
                hasLargerId = true;
                String recipientBrokerName = Config.brokerList.get(i).getHostName();
                Connection connection = brokerConnections.get(recipientBrokerName);
                connection.send(electionMsg.toByteArray());
            }
        }
        if(!hasLargerId){
            //currentBroker will be new leader if there is no other higher broker id
            newLeaderId = hostBrokerId;
            Broker.isElecting = false;
            String dataVersion = Server.buildDataVersion(msgLists);
            MsgInfo.Msg coordinatorMsg = MsgInfo.Msg.newBuilder().setType("coordinator").setLeaderId(newLeaderId).
            setDataVersion(dataVersion).setSenderName(hostBrokerName).build();
            for(int i : liveMembersId){
                String recipientBrokerName = Config.brokerList.get(i).getHostName();
                logger.info("bully algo line 39: send coordinator msg to + " + recipientBrokerName);
                Connection connection = brokerConnections.get(recipientBrokerName);
                Thread t = new Thread(() -> sendCoordinatorMsg(connection, coordinatorMsg));
                t.start();
            }
            logger.info("bully algo line 44: send coordinator msg to load balancer from" + newLeaderId);
            connectionToLoadBalancer.send(coordinatorMsg.toByteArray());

        }
        return newLeaderId;

    }

    /**
     * helper to send coordinator msg to load balancer
     */
    public static void sendCoordinatorMsg(Connection conn, MsgInfo.Msg coordinatorMsg){
        conn.send(coordinatorMsg.toByteArray());
    }
}
