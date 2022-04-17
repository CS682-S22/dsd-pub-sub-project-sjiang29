package framework;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import network.Connection;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import proto.MsgInfo;
import service.*;
import utils.Config;
import utils.HostInfo;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Broker class:  broker to communicate with either producer or consumer and deal their corresponding request
 */
public class Broker {
    public static  Logger logger = LogManager.getLogger();
    public static volatile boolean isElecting = false;
    private String brokerName;
    private int brokerId;
    private ServerSocket server;
    private int brokerPort;
    private volatile boolean isRunning;
    // key is topic, value is msg list of corresponding topic
    private ConcurrentHashMap<String, ArrayList<MsgInfo.Msg>> msgLists;
    // key is the name of the other end of connection
    private ConcurrentHashMap<String, Connection> connections;
    private ConcurrentHashMap<String, Connection> hbConnections;

    private Hashtable<Integer, Long> receivedHeartBeatTime;
    private Membership membership;
    private volatile int numOfSuccessCopy = 0;

    /**
     * Constructor
     * @param brokerName
     */
    public Broker(String brokerName) {
        this.brokerName = brokerName;
        this.brokerId = Config.nameToId.get(this.brokerName);
        this.msgLists = new ConcurrentHashMap<>();
        this.connections = new ConcurrentHashMap<>();
        this.hbConnections = new ConcurrentHashMap<>();
        this.isRunning = true;
        this.brokerPort = Config.hostList.get(brokerName).getPort();
        try {
            //starting broker server
            this.server = new ServerSocket(this.brokerPort);
            //connectToOtherBrokers();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void connectToOtherBrokers(){
        while(true){
            try{

                for(int id : Config.brokerList.keySet()) {
                    if (id != this.brokerId) {
                        HostInfo hostInfo = Config.hostList.get(id);
                        String connectedBrokerAddress = hostInfo.getHostAddress();
                        String connectedBrokerName = hostInfo.getHostName();
                        int connectedBrokerId = hostInfo.getId();
                        int connectedBrokerPort = hostInfo.getPort();
                        Socket socket = new Socket(connectedBrokerAddress, connectedBrokerPort);
                        Connection connection = new Connection(socket);
                        this.hbConnections.put(connectedBrokerName, connection);
                        this.membership.markAlive(connectedBrokerId);
                   }
                }
                sendAndReceiveHb();
                break;
            }catch (IOException e){
                try {
                    Thread.sleep(500);
                } catch (InterruptedException ex) {
                    ex.printStackTrace();
                }
                e.printStackTrace();
            }
        }
    }

    public void sendAndReceiveHb() {
        Set<Integer> allMembers = this.membership.getAllMembers();
        for(int brokerMemberId : allMembers){
            String connectedBrokerName = Config.brokerList.get(brokerMemberId).getHostName();
            Connection connection = this.hbConnections.get(connectedBrokerName);

            HeartBeatSender hbSender = new HeartBeatSender(connection, this.brokerId, this.brokerName);
            HeartBeatScheduler hbScheduler = new HeartBeatScheduler(hbSender, 3000);
            hbScheduler.start();
            HeartBeatReceiver hbReceiver = new HeartBeatReceiver(connection, this.receivedHeartBeatTime, this.membership);
            hbReceiver.run();
        }


    }



    /**
     * Getter to get brokerPort(for auto test purpose)
     * @return
     */
    public int getBrokerPort() {
        return brokerPort;
    }

    /**
     * Method for starting a broker and receive unlimited connections
     */
    public void startBroker(){
        this.isRunning = true;
        while(this.isRunning){
            Thread t = new Thread(() -> connectToOtherBrokers());
            t.start();

            Connection connection = this.buildNewConnection();
            Thread connectionHandler = new Thread(new ConnectionHandler(connection));
            connectionHandler.start();
        }
    }

    /**
     * Method for shutting down a broker
     */
    public void shutDownBroker(){
        this.isRunning = false;
    }

    /**
     * Listens to new socket connection, return corresponding connection according to value of delay and lossRate
     * @return see method description
     */
    public Connection buildNewConnection() {
        Socket socket = null;
        try {
            socket = this.server.accept();
        } catch (IOException e) {
            e.printStackTrace();
        }

        logger.info("broker's line 84: someone is calling");
        Connection connection = new Connection(socket);
        return connection;
    }

    public boolean isLeader(){
        //int currentBrokerId = this.membership.getId(this.brokerName);
        if(this.brokerId == this.membership.getLeaderId()){
            return true;
        } else {
            return false;
        }
    }
    /**
     * Inner ConnectionHandler class:  an inner helper runnable class to deal a specific connection
     */
    class ConnectionHandler implements Runnable{
        private Connection connection;

        /**
         * Constructor
         * @param connection
         */
        public ConnectionHandler(Connection connection) {
            this.connection = connection;
        }

        /**
         * Runnable interface method
         */
        @Override
        public void run() {
            while(isRunning){
                byte[] receivedBytes = this.connection.receive();
                try {
                    MsgInfo.Msg receivedMsg = MsgInfo.Msg.parseFrom(receivedBytes);
                    String senderName = receivedMsg.getSenderName();
                    connections.put(senderName, this.connection);
                    logger.info("broker line 111: senderName + " + senderName);
                    String type = receivedMsg.getType();
                    // if msg type is subscribe and sender is a consumer, use dealConsumerReq, else use dealProducerReq
                    if(isConsumerReq(type, senderName)) {
                        dealConsumerReq(receivedMsg);
                    } else if(isProducerReq(type, senderName)) {
                        dealProducerReq(receivedMsg);
                    } else if(isBrokerReq(type, senderName)) {
                        dealBrokerReq(receivedMsg, this.connection);
                    }
                } catch (InvalidProtocolBufferException e) {
                    e.printStackTrace();
                }
            }
        }

//        private boolean isLeader(){
//            int currentBrokerId = membership.getId(brokerName);
//            if(currentBrokerId == membership.getLeaderId()){
//                return true;
//            } else {
//                return false;
//            }
//        }

        private void redirectToLeader(){

        }

        private boolean isConsumerReq(String type, String senderName){
            return type.equals("subscribe") && senderName.contains("consumer");
        }

        private boolean isProducerReq(String type, String senderName){
            return type.equals("publish") && senderName.contains("producer");
        }

        private boolean isBrokerReq(String type, String senderName){
            boolean isBrokerReqType = type.equals("HeartBeat") || type.equals("election") || type.equals("coordinator")
                    || type.equals("copy") || type.equals("successfulCopy");
            return isBrokerReqType && senderName.contains("broker");
        }

        /**
         * Helper method to deal consumer's request
         * @param receivedMsg
         */
        private void dealConsumerReq(MsgInfo.Msg receivedMsg) {
            String subscribedTopic = receivedMsg.getTopic();
            int startingPosition = receivedMsg.getStartingPosition();
            int requiredMsgCount = receivedMsg.getRequiredMsgCount();
            logger.info("broker line 133: subscribedTopic: " + subscribedTopic);

            ArrayList<MsgInfo.Msg> requiredMsgList = msgLists.get(subscribedTopic);
            if(requiredMsgList == null){
                MsgInfo.Msg responseMsg = MsgInfo.Msg.newBuilder().setType("unavailable").setSenderName(brokerName).build();
                this.connection.send(responseMsg.toByteArray());
            }else{
                // send Msg one by one
                MsgInfo.Msg requiredMsg;
                int endPoint;
                if(requiredMsgList.size() > startingPosition + requiredMsgCount){
                    endPoint = startingPosition + requiredMsgCount;
                } else {
                    endPoint = requiredMsgList.size();
                }
                for(int i = startingPosition; i < endPoint; i++){
                    requiredMsg = MsgInfo.Msg.newBuilder().setType("result").setContent(requiredMsgList.get(i).getContent()).build();
                    logger.info("broker 144, response msg : " + new String(requiredMsg.getContent().toByteArray()));
                    this.connection.send(requiredMsg.toByteArray());
                }
                MsgInfo.Msg stopMsg = MsgInfo.Msg.newBuilder().setType("stop").build();
                this.connection.send(stopMsg.toByteArray());
            }
        }

        /**
         * Helper method to deal producer's request
         * @param receivedMsg
         */
        private void dealProducerReq(MsgInfo.Msg receivedMsg){
            String publishedTopic = receivedMsg.getTopic();
            logger.info("broker line 157: publishedTopic + " + publishedTopic);
            ArrayList<MsgInfo.Msg> messages = msgLists.get(publishedTopic);
            if(messages == null){
                messages = new ArrayList<>();
            }
            messages.add(receivedMsg);
            msgLists.put(publishedTopic, messages);
        }

        private void dealBrokerReq(MsgInfo.Msg receivedMsg, Connection connection){
            String type = receivedMsg.getType();
            String senderName = receivedMsg.getSenderName();
            if(type.equals("HeartBeat")){
                long currentTime = System.nanoTime();
                int id = receivedMsg.getSenderId();
                receivedHeartBeatTime.put(id, currentTime);
                membership.markAlive(id);
            } else if (type.equals("coordinator")){
                int newLeaderId = Config.nameToId.get(senderName);
                membership.setLeaderId(newLeaderId);
            } else if (type.equals("election")){
                int newLeaderId = BullyAlgo.sendBullyReq(membership, brokerName, connections);
                if(newLeaderId != -1){
                    membership.setLeaderId(newLeaderId);
                }
            } else if(type.equals("copy")){
                dealProducerReq(receivedMsg);
                MsgInfo.Msg successfulCopyMsg = MsgInfo.Msg.newBuilder().setType("successfulCopy").setSenderName(brokerName).build();
                connection.send(successfulCopyMsg.toByteArray());
            } else if(type.equals("successfulCopy")){
                numOfSuccessCopy++;

            }

        }
    }





}
