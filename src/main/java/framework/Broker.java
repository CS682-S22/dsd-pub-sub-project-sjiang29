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
    //current broker info
    public static  Logger logger = LogManager.getLogger();
    public static volatile boolean isElecting;
    private String brokerName;
    private int brokerId;
    // push or pull based
    private String brokerType;
    private ServerSocket server;
    private int brokerPort;

    private volatile boolean isRunning;
    private int startingPos;
    // key is topic, value is msg list of corresponding topic
    private ConcurrentHashMap<String, CopyOnWriteArrayList<MsgInfo.Msg>> msgLists;
    // key is topic, value is list of consumers who subscribe this topic
    private ConcurrentHashMap<String, ArrayList<String>> subscriberList;
    // data version from current broker and its followers
    private ConcurrentHashMap<String, String> dataVersions;
    //private CopyOnWriteArrayList<String> dataVersions;
    // key is the name of the other end of connection
    private ConcurrentHashMap<String, Connection> connections;
    private ConcurrentHashMap<String, Connection> brokerConnections;
    private ConcurrentHashMap<String, Connection> loadBalancerConnections;
    //private Connection connectionToLoadBalancer;

    private ConcurrentHashMap<Integer, Long> receivedHeartBeatTime;
    private Membership membership;
    private HeartBeatScheduler failureDetector;

    // key is producer name
    private ConcurrentHashMap<String, CopyStatus> copyStatuses;
    //key is producer, value is producer name
    private ConcurrentHashMap<String, String> topicToClient;

    /**
     * Constructor
     * @param brokerName
     */
    public Broker(String brokerName, String brokerType) {
        this.brokerName = brokerName;
        this.brokerType = brokerType;
        this.startingPos = 0;
        this.brokerId = Config.nameToId.get(this.brokerName);
        isElecting = false;
        this.msgLists = new ConcurrentHashMap<>();
        this.subscriberList = new ConcurrentHashMap<>();
        this.dataVersions = new ConcurrentHashMap<>();
        this.copyStatuses = new ConcurrentHashMap<>();
        this.topicToClient = new ConcurrentHashMap<>();
        this.receivedHeartBeatTime = new ConcurrentHashMap<>();
        this.membership = new Membership();
        this.connections = new ConcurrentHashMap<>();
        this.brokerConnections = new ConcurrentHashMap<>();
        this.loadBalancerConnections = new ConcurrentHashMap<>();

        this.failureDetector = new HeartBeatScheduler(new HeartBeatChecker(this.brokerName, this.receivedHeartBeatTime,5000000000L,
                this.membership, this.brokerConnections, this.loadBalancerConnections, this.msgLists), 7000);
        this.isRunning = true;

        this.brokerPort = Config.hostList.get(brokerName).getPort();
        try {
            //starting broker server
            logger.info("broker line 64: broker starts at port: " + this.brokerPort);
            this.server = new ServerSocket(this.brokerPort);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Helper to build connections between current broker and all load balancers
     */
    private void connectToLoadBalancers(){
        for(String loadBalancerName: Config.loadBalancerList.keySet()){
            Connection connection = Server.connectToLoadBalancer(loadBalancerName, this.brokerName);
            this.loadBalancerConnections.put(loadBalancerName, connection);
            int loadBalancerId = Config.loadBalancerList.get(loadBalancerName).getId();
            this.membership.markAlive(loadBalancerId);
            Thread connectionHandler = new Thread(new ConnectionHandler(connection));
            connectionHandler.start();
        }
    }

    /**
     * Helper to build connections to other brokers on config
     */
    private void connectToOtherBrokers(){
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


    /**
     * Helper to send heart beat to other brokers
     */
    public void sendHb() {
        Set<Integer> allBrokers = this.membership.getAllLiveBrokers();
        logger.info("broker line 112 isElecting: " + this.isElecting);
        if(this.isElecting == false){
            for(int brokerMemberId : allBrokers){
                logger.info("broker line 107: send hb to: " + brokerMemberId);
                String connectedBrokerName = Config.brokerList.get(brokerMemberId).getHostName();
                Connection connection = this.brokerConnections.get(connectedBrokerName);

                HeartBeatSender hbSender = new HeartBeatSender(connection, this.brokerId, this.brokerName);
                HeartBeatScheduler hbScheduler = new HeartBeatScheduler(hbSender, 2000);
                hbScheduler.start();
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
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        connectToLoadBalancers();
        connectToOtherBrokers();
        sendHb();
        this.failureDetector.start();

        while(this.isRunning){
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
     * Method to check if current broker is leader, return true is yes
     * @return  see method description
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
                    //logger.info("broker line 223: senderName + " + senderName + "**type: " + type);
                    if(isConsumerReq(type, senderName)) {
                        dealConsumerReq(receivedMsg, senderName);
                    } else if(isProducerReq(type, senderName)) {
                        dealProducerReq(receivedMsg);
                    } else if(isBrokerReq(type, senderName)) {
                        dealBrokerReq(receivedMsg, this.connection);
                    } else if(isLoadBalancerReq(type, senderName)) {
                        dealLoadBalancerReq(receivedMsg, this.connection);
                    }
                } catch (InvalidProtocolBufferException e) {
                    e.printStackTrace();
                }
            }
        }


        /**
         * Method to check if a coming request is a consumer request, return true if yes
         * @return see method description
         */
        private boolean isConsumerReq(String type, String senderName){
            return type.equals("subscribe") && senderName.contains("consumer");
        }

        /**
         * Method to check if a coming request is a producer request, return true if yes
         * @return see method description
         */
        private boolean isProducerReq(String type, String senderName){
            return (type.equals("publish") || (type.equals("copyNum"))) && senderName.contains("producer");
        }

        /**
         * Method to check if a coming request is a broker request, return true if yes
         * @return see method description
         */
        private boolean isBrokerReq(String type, String senderName){
            boolean isBrokerReqType = type.equals("HeartBeat") || type.equals("election") || type.equals("coordinator") ||
                    type.equals("dataVersion") || type.equals("copy") || type.equals("successfulCopy") || type.equals("subscriber")
                    || type.equals("earliestDataVersion") || type.equals("sync")
                    || type.equals("latestDataVersion") || type.equals("askForMsg") || type.equals("requiredMsg");
            return isBrokerReqType && senderName.contains("broker");
        }


        /**
         * Method to check if a coming request is a load balancer request, return true if yes
         * @return see method description
         */
        private boolean isLoadBalancerReq(String type, String senderName){
            boolean isLoadBalancerReqType = type.equals("HeartBeat");
            return isLoadBalancerReqType && senderName.contains("loadBalancer");
        }

        /**
         * Helper method to deal consumer's request
         * @param receivedMsg
         */
        private void dealConsumerReq(MsgInfo.Msg receivedMsg, String senderName) {
            String subscribedTopic = receivedMsg.getTopic();
            int startingPosition = receivedMsg.getStartingPosition();
            int requiredMsgCount = receivedMsg.getRequiredMsgCount();
            startingPos = startingPosition;
            logger.info("broker line 133: subscribedTopic: " + subscribedTopic);

            if(isElecting == true) {
                MsgInfo.Msg responseMsg = MsgInfo.Msg.newBuilder().setType("unavailable").setSenderName(brokerName).build();
                this.connection.send(responseMsg.toByteArray());
            } else {
                if(brokerType.equals("pull")){
                    dealPullConsumerReq(subscribedTopic, startingPosition, requiredMsgCount);
                } else if(brokerType.equals("push")){
                    dealPushConsumerReq(subscribedTopic,startingPosition,senderName);
                }

            }
        }

        /**
         * Helper method to deal consumer's request(pull based)
         * @param subscribedTopic
         * @param startingPosition
         * @param requiredMsgCount
         */
        private void dealPullConsumerReq(String subscribedTopic, int startingPosition, int requiredMsgCount){
            CopyOnWriteArrayList<MsgInfo.Msg> requiredMsgList = msgLists.get(subscribedTopic);
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
         * Helper method to deal consumer's request(push based)
         * @param subscribedTopic
         * @param startingPosition
         * @param senderName
         */
        private void dealPushConsumerReq(String subscribedTopic, int startingPosition, String senderName) {
            addNewSubscriber(subscribedTopic,senderName);
            // also need to let followers add this new subscriber
            MsgInfo.Msg subscriber = MsgInfo.Msg.newBuilder().setTopic(subscribedTopic).setType("subscriber")
                    .setSubscriber(senderName).setSenderName(brokerName).build();
            sendToFollowers(subscriber);

        }

        /**
         * Helper to add newly coming subscriber to corresponding subscriber list based on subscribed topic
         * @param newSubscriber
         * @param subscribedTopic
         */
        private void addNewSubscriber(String subscribedTopic, String newSubscriber){
            ArrayList<String> subscribers = subscriberList.get(subscribedTopic);
            if(subscribers == null){
                subscribers = new ArrayList<>();
            }
            subscribers.add(newSubscriber);
            logger.info("broker line 135: subscriber name + " + newSubscriber);
            subscriberList.put(subscribedTopic, subscribers);
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

                            // if is push based, send new coming msg to subscribers instantly
                            if(brokerType.equals("push") && messages.size() >= startingPos){
                                ArrayList<String> subscribers = subscriberList.get(publishedTopic);
                                if(subscribers != null){
                                    for(String subscriber : subscribers){
                                        logger.info("broker line 164: subscriber " + subscriber);
                                        Connection connection = connections.get(subscriber);
                                        MsgInfo.Msg requiredMsg = MsgInfo.Msg.newBuilder().setType("result").setContent(receivedMsg.getContent()).build();
                                        logger.info("broker 175, response msg : " + new String(requiredMsg.getContent().toByteArray()));
                                        connection.send(requiredMsg.toByteArray());
                                    }
                                }
                            }
                        }
                    }
                } else {
                    logger.info("broker line 331: rejection");
                    MsgInfo.Msg rejectPubMsg = MsgInfo.Msg.newBuilder().setType("rejection").setSenderName(brokerName).build();
                    this.connection.send(rejectPubMsg.toByteArray());
                }

            }
        }

        /**
         * Helper to send some message to one follower
         * @param connection
         * @param msg
         */
        private void sendToFollower(MsgInfo.Msg msg, Connection connection){
            connection.send(msg.toByteArray());
        }


        /**
         * Helper to send some message to all followers
         * @param receivedMsg
         */
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
            }
        }


        /**
         * Helper method to deal broker's request
         * @param receivedMsg
         */
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
                String dataVersion = Server.buildDataVersion(msgLists);
                MsgInfo.Msg dataVersionMsg = MsgInfo.Msg.newBuilder().setType("dataVersion").setDataVersion(dataVersion).setSenderName(brokerName).build();
                this.connection.send(dataVersionMsg.toByteArray());
            } else if(type.equals("dataVersion")) {
                Broker.isElecting = false;
                String currentBrokerDv = Server.buildDataVersion(msgLists);
                dataVersions.put(brokerName, currentBrokerDv);
                String dataVersion = receivedMsg.getDataVersion();
                dataVersions.put(senderName,dataVersion);
                MsgInfo.Msg latestDV = Server.pickLatestDataVersion(dataVersions, brokerName);
                dealLatestDataVersion(latestDV);
                sendLatestToFollowers(latestDV);
                //dealEarliestDataVersion(earliestDV);
                //sendEarliestToFollowers(earliestDV);
            } else if(type.equals("latestDataVersion")) {
                //String earliestDV = receivedMsg.getDataVersion();
                dealLatestDataVersion(receivedMsg);
            } else if(type.equals("askForMsg")) {
                int startingPos = receivedMsg.getStartingPosition();
                int requiredMsgCount = receivedMsg.getRequiredMsgCount();
                String topic = receivedMsg.getTopic();
                sendRequiredMsg(startingPos, requiredMsgCount, topic);

            } else if(type.equals("requiredMsg")) {
                saveRequiredMsg(receivedMsg);
            } else if (type.equals("election")){
                Broker.isElecting = true;
                int newLeaderId = BullyAlgo.sendBullyReq(membership, brokerName, brokerConnections, loadBalancerConnections, msgLists);
                if(newLeaderId != -1){
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
            } else if(type.equals("subscriber")){
                String subscribedTopic = receivedMsg.getTopic();
                String subscriber = receivedMsg.getSubscriber();
                addNewSubscriber(subscribedTopic, subscriber);
            }

        }



        /**
         * Helper method to deal copy msg from leader broker
         * @param receivedMsg
         */
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

        /**
         * Helper method to send earliestDataVersion among all live brokers to all brokers
         * @param earliestDataVersion
         */
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

        /**
         * Helper method to send latestDataVersion among all live brokers to all brokers
         * @param latestDV
         */
        private void sendLatestToFollowers(MsgInfo.Msg latestDV){
            String latestDataVersion = latestDV.getDataVersion();
            MsgInfo.Msg latestDVMsg = MsgInfo.Msg.newBuilder().setType("latestDataVersion").setDataVersion(latestDataVersion)
                    .setSenderName(brokerName).build();
            ArrayList<Integer> followers = membership.getFollowers(brokerId);
            for(int follower: followers){
                Connection connection = brokerConnections.get(Config.brokerList.get(follower).getHostName());
                logger.info("broker line 327: send to follower");
                Thread t = new Thread(() -> sendToFollower(latestDVMsg, connection));
                t.start();
            }
        }


        /**
         * Helper method to deal lastestDataVersion message type, if left behind, ask for msg from broker who has it
         * @param latestDV
         */
        private void dealLatestDataVersion(MsgInfo.Msg latestDV){
            String latestDataVersion = latestDV.getDataVersion();
            int[] nums = Server.getTopicMsgCount(latestDataVersion);
            int countOfTopic1 = nums[0];
            int countOfTopic2 = nums[1];
            String topic1 = Config.topic1;
            String topic2 = Config.topic2;
            String topic1Owner = latestDV.getTopic1Owner();
            String topic2Owner = latestDV.getTopic2Owner();
            if(countOfTopic1 > msgLists.get(topic1).size()){
                askForMsg(topic1Owner, msgLists.get(topic1).size(), countOfTopic1 -msgLists.get(topic1).size(), topic1 );
            }
            if(countOfTopic2 > msgLists.get(topic2).size()){
                askForMsg(topic2Owner, msgLists.get(topic2).size(), countOfTopic2 -msgLists.get(topic2).size(), topic1 );
            }

        }

        /**
         * Helper method to ask for needed messages from target broker to catch up
         * @param targetBroker
         * @param requiredCount
         * @param startingPos
         * @param topic
         */
        private void askForMsg(String targetBroker, int startingPos, int requiredCount, String topic){
            MsgInfo.Msg requestMsg = MsgInfo.Msg.newBuilder().setType("askForMsg").setTopic(topic).setStartingPosition(startingPos).setRequiredMsgCount(requiredCount)
                    .setSenderName(brokerName).build();
            Connection connection = brokerConnections.get(targetBroker);
            connection.send(requestMsg.toByteArray());
        }


        /**
         * Helper method to send required message to broker who asks for them
         * @param requiredCount
         * @param startingPos
         * @param topic
         */
        private void sendRequiredMsg(int startingPos, int requiredCount, String topic){
            CopyOnWriteArrayList<MsgInfo.Msg> requiredMsgList = msgLists.get(topic);
            MsgInfo.Msg requiredMsg;
            int endPoint;

            endPoint = startingPos + requiredCount;

            for(int i = startingPos; i < endPoint; i++){
                requiredMsg = MsgInfo.Msg.newBuilder().setType("requiredMsg").setContent(requiredMsgList.get(i).getContent()).setTopic(topic).build();
                this.connection.send(requiredMsg.toByteArray());
            }
        }


        /**
         * Helper method to save required message to corresponding list according to topic
         * @param requiredMsg
         */
        private void saveRequiredMsg(MsgInfo.Msg requiredMsg){
            String topic = requiredMsg.getTopic();
            CopyOnWriteArrayList<MsgInfo.Msg> l = msgLists.get(topic);
            l.add(requiredMsg);
            msgLists.put(topic, l);
        }
        /**
         * Helper method to deal earliestDataVersion msg type, roll back to earliest data version accordingly
         * @param earliestDataVersion
         */
        private void dealEarliestDataVersion(String earliestDataVersion){
            int[] nums = Server.getTopicMsgCount(earliestDataVersion);
            int countOfTopic1 = nums[0];
            int countOfTopic2 = nums[1];
            String topic1 = Config.topic1;
            String topic2 = Config.topic2;

            if(countOfTopic1 < msgLists.get(topic1).size()){
                rollBack(countOfTopic1, topic1);
            }

            if(countOfTopic2 < msgLists.get(topic2).size()){
                rollBack(countOfTopic2, topic2);
            }
        }

        /**
         * Helper method to roll back data
         * @param topic topic that needs to be rolled back
         * @param count target count after rolling back
         * */
        private void rollBack(int count, String topic){
            int size = msgLists.get(topic).size();
            int diff = size - count;
            for(int i = 0; i < diff; i++){
                size = msgLists.get(topic).size();
                msgLists.get(topic).remove(size - 1);
            }
        }

        /**
         * Helper method to deal load balancer's request
         * @param connection
         * @param receivedMsg
         */
        private void dealLoadBalancerReq(MsgInfo.Msg receivedMsg, Connection connection){
            String type = receivedMsg.getType();
            String senderName = receivedMsg.getSenderName();
            if(type.equals("HeartBeat")){
                long currentTime = System.nanoTime();
                int id = receivedMsg.getSenderId();
                receivedHeartBeatTime.put(id, currentTime);
                membership.markAlive(id);
            }
        }
    }

}
