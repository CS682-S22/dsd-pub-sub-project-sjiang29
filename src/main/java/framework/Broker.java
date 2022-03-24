package framework;

import com.google.protobuf.InvalidProtocolBufferException;
import network.Connection;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import proto.MsgInfo;
import utils.Config;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Broker class:  broker to communicate with either producer or consumer and deal their corresponding request
 */
public class Broker {
    public static  Logger logger = LogManager.getLogger();
    private String brokerName;
    private int brokerPort;
    private ServerSocket server;
    // key is topic, value is msg list of corresponding topic
    private ConcurrentHashMap<String, ArrayList<MsgInfo.Msg>> msgLists;
    // key is topic, value is list of consumers who subscribe this topic
    private ConcurrentHashMap<String, ArrayList<String>> subscriberList;
    // key is consumer's name, value is its corresponding connection
    private ConcurrentHashMap<String, Connection> connections;

    /**
     * Constructor
     * @param brokerName
     */
    public Broker(String brokerName) {
        this.brokerName = brokerName;
        this.msgLists = new ConcurrentHashMap<>();
        this.subscriberList = new ConcurrentHashMap<>();
        this.connections = new ConcurrentHashMap<>();
        this.brokerPort = Config.hostList.get(brokerName).getPort();
        try {
            //starting broker server
            this.server = new ServerSocket(brokerPort);
        } catch (IOException e) {
            e.printStackTrace();
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
        boolean isListening = true;
        while(isListening){
            Connection connection = this.buildNewConnection();
            Thread connectionHandler = new Thread(new ConnectionHandler(connection));
            connectionHandler.start();
        }
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

        logger.info("broker's line 74: someone is calling");

        Connection connection = new Connection(socket);
        return connection;
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
            boolean isRunning = true;
            while(isRunning){
                byte[] receivedBytes = this.connection.receive();
                try {
                    MsgInfo.Msg receivedMsg = MsgInfo.Msg.parseFrom(receivedBytes);
                    String senderName = receivedMsg.getSenderName();
                    //update connections
                    connections.put(senderName, this.connection);
                    logger.info("broker line 107: senderName + " + senderName);
                    String type = receivedMsg.getType();
                    // if msg type is subscribe and sender is a consumer, use dealConsumerReq, else use dealProducerReq
                    if(type.equals("subscribe") && senderName.contains("consumer")){
                        dealConsumerReq(receivedMsg, senderName);
                    } else if(type.equals("publish") && senderName.contains("producer")) {
                        dealProducerReq(receivedMsg, senderName);
                    }
                } catch (InvalidProtocolBufferException e) {
                    e.printStackTrace();
                }
            }

        }

        /**
         * Helper method to deal consumer's request
         * @param receivedMsg
         */
        private void dealConsumerReq(MsgInfo.Msg receivedMsg, String senderName) {
            String subscribedTopic = receivedMsg.getTopic();
            int startingPosition = receivedMsg.getStartingPosition();
            logger.info("broker line 129: subscribedTopic + " + subscribedTopic);
            ArrayList<String> subscribers = subscriberList.get(subscribedTopic);
            if(subscribers == null){
                subscribers = new ArrayList<>();
            }
            subscribers.add(senderName);
            logger.info("broker line 135: subscriber name + " + senderName);
            subscriberList.put(subscribedTopic, subscribers);
            ArrayList<MsgInfo.Msg> requiredMsgList = msgLists.get(subscribedTopic);
            if(requiredMsgList != null) {
                MsgInfo.Msg requiredMsg;
                for(int i = startingPosition; i < requiredMsgList.size(); i++){
                    requiredMsg = MsgInfo.Msg.newBuilder().setType("result").setContent(requiredMsgList.get(i).getContent()).build();
                    logger.info("broker 142, response msg : " + requiredMsg.getContent());
                    this.connection.send(requiredMsg.toByteArray());
                }
            }
        }

        /**
         * Helper method to deal producer's request
         * @param receivedMsg
         */
        private void dealProducerReq(MsgInfo.Msg receivedMsg, String senderName) {
            String publishedTopic = receivedMsg.getTopic();
            logger.info("broker line 154: publishedTopic + " + publishedTopic);
            ArrayList<MsgInfo.Msg> messages = msgLists.get(publishedTopic);
            if(messages == null){
                messages = new ArrayList<>();
            }
            messages.add(receivedMsg);
            msgLists.put(publishedTopic, messages);
            ArrayList<String> subscribers = subscriberList.get(publishedTopic);
            if(subscribers != null){
                for(String subscriber : subscribers){
                    logger.info("broker line 164: subscriber " + subscriber);
                    Connection connection = connections.get(subscriber);
                    MsgInfo.Msg requiredMsg = MsgInfo.Msg.newBuilder().setType("result").setContent(receivedMsg.getContent()).build();
                    connection.send(requiredMsg.toByteArray());
                }
            }
        }
    }



}
