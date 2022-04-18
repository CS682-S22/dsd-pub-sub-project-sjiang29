package framework;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import network.Connection;
import proto.MsgInfo;
import utils.Config;

import java.io.IOException;
import java.net.Socket;
import java.util.HashMap;

import static framework.Broker.logger;

/**
 * Producer class: class to publish message to broker
 *
 */
public class Producer {
    private String leaderBrokerName;
    private String producerName;
    private volatile int hasNewLeader;
    private Connection leaderBrokerConnection;
    private Connection loadBalancerConnection;
    private int msgId;

    /**
     * Constructor
     * @param leaderBrokerName
     * @param producerName
     *
     */
    public Producer(String leaderBrokerName, String producerName) {
        this.msgId = 1;
        this.leaderBrokerName = leaderBrokerName;
        this.producerName = producerName;
        int leaderBrokerId = Config.nameToId.get(this.leaderBrokerName);

        String leaderBrokerAddress = Config.brokerList.get(leaderBrokerId).getHostAddress();
        int leaderBrokerPort = Config.brokerList.get(leaderBrokerId).getPort();

        String loadBalancerAddress = Config.hostList.get("loadBalancer").getHostAddress();
        int loadBalancerPort = Config.hostList.get("loadBalancer").getPort();

        try {
            Socket socket1 = new Socket(leaderBrokerAddress, leaderBrokerPort);
            this.leaderBrokerConnection = new Connection(socket1);

            Socket socket2 = new Socket(leaderBrokerAddress, leaderBrokerPort);
            this.loadBalancerConnection = new Connection(socket2);
        } catch (IOException e) {
            e.printStackTrace();
        }



    }

    public void updateLeaderBrokerConnection(){
        byte[] receivedBytes = this.loadBalancerConnection.receive();
        try {
            MsgInfo.Msg receivedMsg = MsgInfo.Msg.parseFrom(receivedBytes);
            if(receivedMsg.getType().equals("coordinator")){

            }
        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
        }
    }



    /**
     * Method to send message of some topic to a broker
     * @param topic
     * @param data
     *
     */
    public void send(String topic, byte[] data){
        MsgInfo.Msg sentMsg = MsgInfo.Msg.newBuilder().setTopic(topic).setType("publish")
                .setContent(ByteString.copyFrom(data)).setId(this.msgId++).setSenderName(this.producerName).build();
        this.leaderBrokerConnection.send(sentMsg.toByteArray());
    }

    /**
     * Method to close the connection to a broker
     *
     */
    public void close(){
        this.brokerConnection.close();
    }
}
