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
import java.util.concurrent.ConcurrentHashMap;

import static framework.Broker.logger;

/**
 * Producer class: class to publish message to broker
 *
 */
public class Producer {
    private String leaderBrokerName;
    private String leaderBrokerAddress;
    private int leaderBrokerPort;
    private int leaderBrokerId;
    private String producerName;
    private int copyNum;
    private volatile int numOfSending;
    private volatile int numOfAck;

    private Connection leaderBrokerConnection;
    private Connection loadBalancerConnection;
    private boolean isUpdating;
    private ConcurrentHashMap<String, Connection> loadBalancerConnections;
    private int msgId;



    /**
     * Constructor
     * @param producerName
     *
     */
    public Producer(String producerName, int copyNum) {
        this.msgId = 1;
        this.leaderBrokerName = "broker5";
        this.leaderBrokerId = Config.nameToId.get(this.leaderBrokerName);
        this.leaderBrokerAddress = Config.brokerList.get(this.leaderBrokerId).getHostAddress();
        this.leaderBrokerPort = Config.brokerList.get(this.leaderBrokerId).getPort();

        this.producerName = producerName;
        this.copyNum = copyNum;
        this.numOfSending = 0;
        this.numOfAck = 0;
        this.loadBalancerConnection = null;
        this.loadBalancerConnections = new ConcurrentHashMap<>();
        this.isUpdating = false;
        Server.connectToLoadBalancers(loadBalancerConnections, this.producerName);

        try {
            Socket socket = new Socket(this.leaderBrokerAddress, this.leaderBrokerPort);
            this.leaderBrokerConnection = new Connection(socket);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    /**
     * Helper to send required copy number to leader broker
     * @param copyNum
     */
    public void sendCopyNum(int copyNum){
        MsgInfo.Msg requestMsg = MsgInfo.Msg.newBuilder().setType("copyNum").setCopyNum(copyNum).setSenderName(this.producerName).build();
        logger.info("producer line 67: send num copy to" + this.leaderBrokerName );
        this.leaderBrokerConnection.send(requestMsg.toByteArray());
    }

    /**
     * Helper to update leader connection if old leader fails and new leader if prompted
     */
    public void updateLeaderBrokerConnection(){

        byte[] receivedBytes = null;
        for(String loadBalancerName : this.loadBalancerConnections.keySet()){
            logger.info("producer line 85: " );
            Connection connection = loadBalancerConnections.get(loadBalancerName);
            logger.info("producer line 88: " );
            receivedBytes = connection.receive();
            logger.info("producer line 89: " );
            if(receivedBytes != null){
                logger.info("producer line 90: " );
                this.loadBalancerConnection = connection;
                break;
            } else {
                continue;
            }
        }

        try {
            MsgInfo.Msg receivedMsg = MsgInfo.Msg.parseFrom(receivedBytes);

            if(receivedMsg.getType().equals("coordinator")){
                int newLeaderId = receivedMsg.getLeaderId();
                this.leaderBrokerId = newLeaderId;
                logger.info("producer line 70: new leader is promoted, new leader: " + newLeaderId);
                this.leaderBrokerName = Config.brokerList.get(newLeaderId).getHostName();
                this.leaderBrokerAddress = Config.brokerList.get(newLeaderId).getHostAddress();
                this.leaderBrokerPort = Config.brokerList.get(newLeaderId).getPort();
                Socket socket = new Socket(leaderBrokerAddress, leaderBrokerPort);
                this.leaderBrokerConnection = new Connection(socket);
                sendCopyNum(this.copyNum);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

//    public void updateLeaderBrokerConnection(){
//        this.isUpdating = true;
//        //while(this.isUpdating){
//            for(String loadBalancerName : this.loadBalancerConnections.keySet()){
//                logger.info("producer line 85: " );
//                Connection connection = loadBalancerConnections.get(loadBalancerName);
//                Thread connectionHandler = new Thread(new Producer.ConnectionHandler(connection));
//                connectionHandler.start();
//
//            }
//        //}
//
//
//
//    }



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
               this.msgId--;
               logger.info("******producer line 153: failing " );
               updateLeaderBrokerConnection();
               logger.info("******producer line 153: keep sending " );
               this.leaderBrokerConnection.send(sentMsg.toByteArray());
           } else {
               this.numOfSending++;
           }
           logger.info("producer line 94 published line to: " + this.leaderBrokerName);
    }


    /**
     * Helper to check is published msg is received and copied successfully by leader and follower, if not resend
     * @param data
     * @param topic
     */
    public synchronized boolean sendSuccessfully(String topic, byte[] data){
        byte[] receivedBytes;
        try {
            receivedBytes = this.leaderBrokerConnection.receive();
            logger.info("line 120");

            if(receivedBytes == null){
                updateLeaderBrokerConnection();
                    if(this.numOfSending > this.numOfAck){
                        this.send(topic, data);
                    }
                    receivedBytes = this.leaderBrokerConnection.receive();
            }
            MsgInfo.Msg receivedMsg = MsgInfo.Msg.parseFrom(receivedBytes);
            String type = receivedMsg.getType();
            if(type.equals("acknowledge")){
                this.numOfAck++;
                return true;
            } else if(type.equals("rejection")){
                return false;
            }
        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
        }
        return false;
    }

    class ConnectionHandler implements Runnable{

        private Connection connection;

        /**
         * Constructor
         * @param connection
         */
        public ConnectionHandler(Connection connection) {
            this.connection = connection;
        }

        @Override
        public void run() {
            boolean isRunning = true;
            //while(isRunning){
                byte[] receivedBytes = this.connection.receive();
                try {
                    MsgInfo.Msg receivedMsg = MsgInfo.Msg.parseFrom(receivedBytes);
                    if(receivedMsg.getType().equals("coordinator")){
                        int newLeaderId = receivedMsg.getLeaderId();
                        leaderBrokerId = newLeaderId;
                        logger.info("producer line 70: new leader is promoted, new leader: " + newLeaderId);
                        leaderBrokerName = Config.brokerList.get(newLeaderId).getHostName();
                        leaderBrokerAddress = Config.brokerList.get(newLeaderId).getHostAddress();
                        leaderBrokerPort = Config.brokerList.get(newLeaderId).getPort();
                        Socket socket = new Socket(leaderBrokerAddress, leaderBrokerPort);
                        leaderBrokerConnection = new Connection(socket);
                        sendCopyNum(copyNum);
                        isUpdating = false;
                        //isRunning = false;
                    }

                } catch (InvalidProtocolBufferException e) {
                    e.printStackTrace();
                } catch (UnknownHostException e) {
                    e.printStackTrace();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            //}
        }
    }

}
