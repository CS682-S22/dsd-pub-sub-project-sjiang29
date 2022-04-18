package service;

import network.Connection;
import proto.MsgInfo;
import utils.Config;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

public class BullyAlgo {

    public static int sendBullyReq(Membership membership, String hostBrokerName, ConcurrentHashMap<String, Connection> connections){

        int newLeaderId = -1;
        boolean hasLargerId = false;
        CopyOnWriteArrayList<Integer> liveMembersId = membership.getAllMembers();
        int hostBrokerId = Config.nameToId.get(hostBrokerName);
        MsgInfo.Msg electionMsg = MsgInfo.Msg.newBuilder().setType("election").setSenderName(hostBrokerName).build();
        for(int i : liveMembersId){
            if(i > hostBrokerId) {
                hasLargerId = true;
                String recipientBrokerName = Config.hostList.get(i).getHostName();
                Connection connection = connections.get(recipientBrokerName);
                connection.send(electionMsg.toByteArray());
            }
        }
        if(!hasLargerId){
            //currentBroker will be new leader if there is no other higher broker id
            newLeaderId = hostBrokerId;
            MsgInfo.Msg coordinatorMsg = MsgInfo.Msg.newBuilder().setType("coordinator").setSenderName(hostBrokerName).build();
            for(int i : liveMembersId){
                String recipientBrokerName = Config.hostList.get(i).getHostName();
                Connection connection = connections.get(recipientBrokerName);
                connection.send(coordinatorMsg.toByteArray());
            }

        }
        return newLeaderId;

    }
}
