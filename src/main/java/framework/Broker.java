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
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Broker class:  broker to communicate with either producer or consumer and deal their corresponding request
 */
public class Broker {
    public static  Logger logger = LogManager.getLogger();
    public static volatile boolean isElecting;
    private String brokerName;
    private int brokerId;
    private ServerSocket server;
    private int brokerPort;


    private volatile boolean isRunning;
    private volatile boolean isSync;
    // key is topic, value is msg list of corresponding topic
    private ConcurrentHashMap<String, CopyOnWriteArrayList<MsgInfo.Msg>> msgLists;
    private CopyOnWriteArrayList<String> dataVersions;
    // key is the name of the other end of connection
    private ConcurrentHashMap<String, Connection> connections;
    private ConcurrentHashMap<String, Connection> brokerConnections;
    private Connection connectionToLoadBalancer;

    private ConcurrentHashMap<Integer, Long> receivedHeartBeatTime;
    private Membership membership;
    private HeartBeatScheduler failureDetector;

    // key is producer name
    private ConcurrentHashMap<String, CopyStatus> copyStatuses;
    private ConcurrentHashMap<String, String> topicToClient;

    /**
     * Constructor
     * @param brokerName
     */
    public Broker(String brokerName) {
        this.brokerName = brokerName;
        this.brokerId = Config.nameToId.get(this.brokerName);
        isElecting = false;
        this.msgLists = new ConcurrentHashMap<>();
        this.dataVersions = new CopyOnWriteArrayList<>();
        this.copyStatuses = new ConcurrentHashMap<>();
        this.topicToClient = new ConcurrentHashMap<>();
        this.receivedHeartBeatTime = new ConcurrentHashMap<>();
        this.membership = new Membership();
        this.connections = new ConcurrentHashMap<>();
        this.brokerConnections = new ConcurrentHashMap<>();
        this.connectionToLoadBalancer = Server.connectToLoadBalancer(this.brokerName);
        this.connections.put("loadBalancer", connectionToLoadBalancer);

        this.failureDetector = new HeartBeatScheduler(new HeartBeatChecker(this.brokerName, this.receivedHeartBeatTime,8000000000L,
                this.membership, this.brokerConnections, this.connectionToLoadBalancer, this.msgLists), 8000);
        this.isRunning = true;
        this.isSync = false;
        this.brokerPort = Config.hostList.get(brokerName).getPort();
        try {
            //starting broker server
            logger.info("broker line 64: broker starts at port: " + this.brokerPort);
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
                        HostInfo hostInfo = Config.brokerList.get(id);
                        String connectedBrokerAddress = hostInfo.getHostAddress();
                        String connectedBrokerName = hostInfo.getHostName();
                        int connectedBrokerId = hostInfo.getId();
                        int connectedBrokerPort = hostInfo.getPort();
                        Socket socket = new Socket(connectedBrokerAddress, connectedBrokerPort);
                        Connection connection = new Connection(socket);
                        this.brokerConnections.put(connectedBrokerName, connection);
                        this.membership.markAlive(connectedBrokerId);
                    }
                }

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

    private void connectToBroker(String connectedBrokerAddress, int connectedBrokerPort,
                                 int connectedBrokerId,String connectedBrokerName){
        Socket socket = null;
        try {
            socket = new Socket(connectedBrokerAddress, connectedBrokerPort);
            Connection connection = new Connection(socket);
            this.brokerConnections.put(connectedBrokerName, connection);
            this.membership.markAlive(connectedBrokerId);
        } catch (IOException e) {
            connectToBroker(connectedBrokerAddress, connectedBrokerPort, connectedBrokerId, connectedBrokerName);
            e.printStackTrace();
        }

    }

    public void sendHb() {
        Set<Integer> allMembers = this.membership.getAllMembers();
        logger.info("broker line 112 isElecting: " + this.isElecting);
        if(this.isElecting == false){
            for(int brokerMemberId : allMembers){
                logger.info("broker line 107: send hb to: " + brokerMemberId);
                String connectedBrokerName = Config.brokerList.get(brokerMemberId).getHostName();
                Connection connection = this.brokerConnections.get(connectedBrokerName);

                HeartBeatSender hbSender = new HeartBeatSender(connection, this.brokerId, this.brokerName);
                HeartBeatScheduler hbScheduler = new HeartBeatScheduler(hbSender, 2000);
                hbScheduler.start();
                //HeartBeatReceiver hbReceiver = new HeartBeatReceiver(connection, this.receivedHeartBeatTime, this.membership);
                //hbReceiver.run();
            }
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
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        connectToOtherBrokers();
        sendHb();
        this.failureDetector.start();

        while(this.isRunning){
            //Thread t = new Thread(() -> connectToOtherBrokers());
            //t.start();
            logger.info("broker line 141: connect to other brokers" );

            Connection connection = Server.buildNewConnection(this.server);

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
                    String type = receivedMsg.getType();
                    logger.info("broker line 223: senderName + " + senderName + "**type: " + type);

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


        private boolean isConsumerReq(String type, String senderName){
            return type.equals("subscribe") && senderName.contains("consumer");
        }

        private boolean isProducerReq(String type, String senderName){
            return (type.equals("publish") || (type.equals("copyNum"))) && senderName.contains("producer");
        }

        private boolean isBrokerReq(String type, String senderName){
            boolean isBrokerReqType = type.equals("HeartBeat") || type.equals("election") || type.equals("coordinator") ||
                    type.equals("dataVersion") || type.equals("copy") || type.equals("successfulCopy")
                    || type.equals("earliestDataVersion") || type.equals("sync");
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

            if(isElecting == true) {
                MsgInfo.Msg responseMsg = MsgInfo.Msg.newBuilder().setType("unavailable").setSenderName(brokerName).build();
                this.connection.send(responseMsg.toByteArray());
            } else {
                CopyOnWriteArrayList<MsgInfo.Msg> requiredMsgList = msgLists.get(subscribedTopic);
                if(requiredMsgList == null){

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

        }

        /**
         * Helper method to deal producer's request
         * @param receivedMsg
         */
        private void dealProducerReq(MsgInfo.Msg receivedMsg){
            String type = receivedMsg.getType();
            String producerName = receivedMsg.getSenderName();
            if(type.equals("copyNum")){
                int requiredCopyNum = receivedMsg.getCopyNum();
                CopyStatus copyStatus = new CopyStatus(requiredCopyNum);
                copyStatuses.put(producerName, copyStatus);
                logger.info("broker line 295: copyNum + " + requiredCopyNum);
            } else if(type.equals("publish")){
                if(Broker.isElecting == false){
                    String publishedTopic = receivedMsg.getTopic();
                    topicToClient.put(publishedTopic, producerName);
                    logger.info("broker line 298: publishedTopic + " + publishedTopic);
                    CopyOnWriteArrayList<MsgInfo.Msg> messages = msgLists.get(publishedTopic);
                    if(messages == null){
                        messages = new CopyOnWriteArrayList<>();
                    }
                    messages.add(receivedMsg);

                    msgLists.put(publishedTopic, messages);
                    logger.info("broker line 306: is leader? + " + isLeader());
                    if(isLeader()){
                        sendToFollowers(receivedMsg);
                        try {
                            Thread.sleep(200);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                        int numOfSuccessCopy = copyStatuses.get(producerName).getRequiredCopyNum();
                        if((numOfSuccessCopy >= copyStatuses.get(producerName).getRequiredCopyNum())|| (numOfSuccessCopy == membership.getAllMembers().size())){
                            MsgInfo.Msg ackMsg = MsgInfo.Msg.newBuilder().setType("acknowledge").setSenderName(brokerName).build();
                            this.connection.send(ackMsg.toByteArray());
                            copyStatuses.get(producerName).setNumOfSuccessCopy(0);
                        }
                    }
                } else {
                    logger.info("broker line 331: rejection");
                    MsgInfo.Msg rejectPubMsg = MsgInfo.Msg.newBuilder().setType("rejection").setSenderName(brokerName).build();
                    this.connection.send(rejectPubMsg.toByteArray());
                }


            }
        }

        private void sendToFollower(MsgInfo.Msg msg, Connection connection){
            connection.send(msg.toByteArray());
        }
        private void sendToFollowers(MsgInfo.Msg receivedMsg){

            String topic = receivedMsg.getTopic();
            String msgContent = new String(receivedMsg.getContent().toByteArray());
            MsgInfo.Msg copiedMsg = MsgInfo.Msg.newBuilder().setTopic(topic).setType("copy")
                    .setContent(ByteString.copyFrom(msgContent.getBytes(StandardCharsets.UTF_8))).setSenderName(brokerName).build();
            ArrayList<Integer> followers = membership.getFollowers(brokerId);
            for(int follower: followers){
                Connection connection = brokerConnections.get(Config.brokerList.get(follower).getHostName());
                logger.info("broker line 327: send to follower");
                Thread t = new Thread(() -> sendToFollower(copiedMsg, connection));
                t.start();
//                byte[] receivedBytes = this.connection.receive();
//                try {
//                    MsgInfo.Msg replyMsg = MsgInfo.Msg.parseFrom(receivedBytes);
//                    if(replyMsg.getType().equals("successfulCopy")){
//                        logger.info("broker line 332: receive successful from + " + replyMsg.getSenderName());
//                        numOfSuccessCopy++;
//                    }
//                } catch (InvalidProtocolBufferException e) {
//                    e.printStackTrace();
//                }


            }

        }

        private void syncToNewFollower(){
            isSync = true;
            if(isSync){
                for(String topic : msgLists.keySet()){
                    CopyOnWriteArrayList<MsgInfo.Msg> list = msgLists.get(topic);
                    int msgCount = list.size();
                    int i = 0;
                    while(i < msgCount){
                        MsgInfo.Msg msg = list.get(i);
                        MsgInfo.Msg syncMsg = MsgInfo.Msg.newBuilder().setType("sync").setTopic(topic).setSenderName(brokerName).
                                setContent(msg.getContent()).build();
                        this.connection.send(syncMsg.toByteArray());
                    }
                }
            }
            isSync = false;
        }

        private void deadSyncMsg(MsgInfo.Msg receivedMsg){
            dealCopy(receivedMsg);
        }

        private void dealBrokerReq(MsgInfo.Msg receivedMsg, Connection connection){
            String type = receivedMsg.getType();
            String senderName = receivedMsg.getSenderName();
            if(type.equals("HeartBeat")){
                //logger.info("broker line 327: receive hb from + " + senderName);
                long currentTime = System.nanoTime();
                int id = receivedMsg.getSenderId();
                //logger.info("broker line 336: mark time + " + id + currentTime);
                receivedHeartBeatTime.put(id, currentTime);
                membership.markAlive(id);
                if(senderName.contains("new") && isLeader()){
                    Thread t = new Thread(() -> syncToNewFollower());
                    t.start();
                }
            } else if (type.equals("sync") && brokerName.contains("new")) {
                deadSyncMsg(receivedMsg);
            } else if (type.equals("coordinator")){
                //Broker.isElecting = false;
                int newLeaderId = Config.nameToId.get(senderName);
                membership.setLeaderId(newLeaderId);
                String dataVersion = Server.buildDataVersion(msgLists);
                MsgInfo.Msg dataVersionMsg = MsgInfo.Msg.newBuilder().setType("dataVersion").setDataVersion(dataVersion).setSenderName(brokerName).build();
                this.connection.send(dataVersionMsg.toByteArray());

            } else if(type.equals("dataVersion")) {
                Broker.isElecting = false;
                String currentBrokerDv = Server.buildDataVersion(msgLists);
                dataVersions.add(currentBrokerDv);
                String dataVersion = receivedMsg.getDataVersion();
                dataVersions.add(dataVersion);
                String earliestDV = Server.pickEarliestDataVersion(dataVersions);
                dealEarliestDataVersion(earliestDV);
                sendEarliestToFollowers(earliestDV);

            } else if(type.equals("earliestDataVersion")) {
                String earliestDV = receivedMsg.getDataVersion();
                dealEarliestDataVersion(earliestDV);
            } else if (type.equals("election")){
                Broker.isElecting = true;
                int newLeaderId = BullyAlgo.sendBullyReq(membership, brokerName, brokerConnections, connectionToLoadBalancer, msgLists);
                if(newLeaderId != -1){
                    //Broker.isElecting = false;
                    membership.setLeaderId(newLeaderId);
                }
            } else if(type.equals("copy")){
                logger.info("broker line 65: receive copy from + " + senderName);
                dealCopy(receivedMsg);
                String copiedTopic = receivedMsg.getTopic();
                MsgInfo.Msg successfulCopyMsg = MsgInfo.Msg.newBuilder().setType("successfulCopy").setTopic(copiedTopic).setSenderName(brokerName).build();
                Connection conn = brokerConnections.get(senderName);
                conn.send(successfulCopyMsg.toByteArray());
                logger.info("broker line 369 : copy successful " );

            } else if(type.equals("successfulCopy")){
                String copiedTopic = receivedMsg.getTopic();
                logger.info("broker line 357: receive successful from + " + senderName);
                String producerName = topicToClient.get(copiedTopic);
                copyStatuses.get(producerName).incrementSuccessCopy();

            }

        }

        private void dealCopy(MsgInfo.Msg receivedMsg){
            String publishedTopic = receivedMsg.getTopic();
            logger.info("broker line 298: publishedTopic + " + publishedTopic);
            CopyOnWriteArrayList<MsgInfo.Msg> messages = msgLists.get(publishedTopic);
            if(messages == null){
                messages = new CopyOnWriteArrayList<>();
            }
            messages.add(receivedMsg);

            msgLists.put(publishedTopic, messages);
        }

        private void sendEarliestToFollowers(String earliestDataVersion){
            MsgInfo.Msg earliestDVMsg = MsgInfo.Msg.newBuilder().setType("earliestDataVersion").setDataVersion(earliestDataVersion)
                    .setSenderName(brokerName).build();
            ArrayList<Integer> followers = membership.getFollowers(brokerId);
            for(int follower: followers){
                Connection connection = brokerConnections.get(Config.brokerList.get(follower).getHostName());
                logger.info("broker line 327: send to follower");
                Thread t = new Thread(() -> sendToFollower(earliestDVMsg, connection));
                t.start();
            }
        }

        private void dealEarliestDataVersion(String earliestDataVersion){
            int[] nums = Server.getTopicMsgCount(earliestDataVersion);
            int countOfTopic1 = nums[0];
            int countOftopic2 = nums[1];
            String topic1 = Config.topic1;
            String topic2 = Config.topic2;

            if(countOfTopic1 < msgLists.get(topic1).size()){
                rollBack(countOfTopic1, topic1);
            }

            if(countOftopic2 < msgLists.get(topic2).size()){
                rollBack(countOftopic2, topic2);
            }
        }

        private void rollBack(int count, String topic){
            int size = msgLists.get(topic).size();
            int diff = size - count;
            for(int i = 0; i < diff; i++){
                size = msgLists.get(topic).size();
                msgLists.get(topic).remove(size - 1);
            }
        }



    }









}
