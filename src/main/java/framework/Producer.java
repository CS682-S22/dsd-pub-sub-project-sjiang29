package framework;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import network.Connection;
import proto.MsgInfo;
import utils.Config;

import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.HashMap;

import static framework.Broker.logger;

/**
 * Producer class: class to publish message to broker
 *
 */
public class Producer {
    private String leaderBrokerName;
    private String leaderBrokerAddress;
    private int leaderBrokerPort;
    private String producerName;
    private volatile boolean isUpdatingLeader;
    private Connection leaderBrokerConnection;
    private Connection loadBalancerConnection;
    private int msgId;


    /**
     * Constructor
     * @param producerName
     *
     */
    public Producer(String producerName) {
        this.msgId = 1;
        this.leaderBrokerName = "broker5";
        this.producerName = producerName;
        this.isUpdatingLeader = false;
        int leaderBrokerId = Config.nameToId.get(this.leaderBrokerName);

        this.leaderBrokerAddress = Config.brokerList.get(leaderBrokerId).getHostAddress();
        this.leaderBrokerPort = Config.brokerList.get(leaderBrokerId).getPort();

        this.loadBalancerConnection = Server.connectToLoadBalancer(this.producerName);

        try {
            Socket socket = new Socket(leaderBrokerAddress, leaderBrokerPort);
            this.leaderBrokerConnection = new Connection(socket);

        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public void sendCopyNum(int copyNum){

        MsgInfo.Msg requestMsg = MsgInfo.Msg.newBuilder().setType("copyNum").setCopyNum(copyNum).setSenderName(this.producerName).build();
        this.leaderBrokerConnection.send(requestMsg.toByteArray());
    }

    public void updateLeaderBrokerConnection(){

        byte[] receivedBytes = this.loadBalancerConnection.receive();
        try {
            MsgInfo.Msg receivedMsg = MsgInfo.Msg.parseFrom(receivedBytes);

            if(receivedMsg.getType().equals("coordinator")){

                this.isUpdatingLeader = true;

                int newLeaderId = receivedMsg.getLeaderId();
                logger.info("producer line 70: new leader is promoted, new leader: " + newLeaderId);
                this.leaderBrokerName = Config.brokerList.get(newLeaderId).getHostName();
                this.leaderBrokerAddress = Config.brokerList.get(newLeaderId).getHostAddress();
                this.leaderBrokerPort = Config.brokerList.get(newLeaderId).getPort();
                Socket socket = new Socket(leaderBrokerAddress, leaderBrokerPort);
                this.leaderBrokerConnection = new Connection(socket);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }



    /**
     * Method to send message of some topic to a broker
     * @param topic
     * @param data
     *
     */
    public synchronized void send(String topic, byte[] data){

           MsgInfo.Msg sentMsg = MsgInfo.Msg.newBuilder().setTopic(topic).setType("publish")
                   .setContent(ByteString.copyFrom(data)).setId(this.msgId++).setSenderName(this.producerName).build();
           boolean sendingRes = this.leaderBrokerConnection.send(sentMsg.toByteArray());
           if(sendingRes == false){

           }
           logger.info("producer line 94 published line ");

    }

    public synchronized boolean sendSuccessfully(){


        byte[] receivedBytes = this.leaderBrokerConnection.receive();
        try {
            MsgInfo.Msg receivedMsg = MsgInfo.Msg.parseFrom(receivedBytes);
            String type = receivedMsg.getType();
            if(type.equals("acknowledge")){
                return true;
            }
        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
            updateLeaderBrokerConnection();
            sendSuccessfully();
        }
        return false;
    }



    /**
     * Method to close the connection to a broker
     *
     */

}
