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
    public static volatile boolean isElecting = false;
    private String brokerName;
    private ServerSocket server;
    private int brokerPort;
    private boolean isRunning;
    // key is topic, value is msg list of corresponding topic
    private ConcurrentHashMap<String, ArrayList<MsgInfo.Msg>> msgLists;

    /**
     * Constructor
     * @param brokerName
     */
    public Broker(String brokerName) {
        this.brokerName = brokerName;
        this.msgLists = new ConcurrentHashMap<>();
        this.isRunning = true;
        this.brokerPort = Config.hostList.get(brokerName).getPort();
        try {
            //starting broker server
            this.server = new ServerSocket(this.brokerPort);
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
        this.isRunning = true;
        while(this.isRunning){
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
                    logger.info("broker line 111: senderName + " + senderName);
                    String type = receivedMsg.getType();
                    // if msg type is subscribe and sender is a consumer, use dealConsumerReq, else use dealProducerReq
                    if(type.equals("subscribe") && senderName.contains("consumer")){
                        dealConsumerReq(receivedMsg);
                    } else if(type.equals("publish") && senderName.contains("producer")) {
                        dealProducerReq(receivedMsg);
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
    }





}
