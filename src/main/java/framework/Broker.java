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


public class Broker {
    public static  Logger logger = LogManager.getLogger();
    private String brokerName;
    private ServerSocket server;
    // key is topic, value is msg list of corresponding topic
    private ConcurrentHashMap<String, ArrayList<MsgInfo.Msg>> msgLists;
    // key is topic, value is list of consumers who subscribe this topic
    private ConcurrentHashMap<String, ArrayList<String>> subscriberList;
    // key is consumer's name, value is its corresponding connection
    private ConcurrentHashMap<String, Connection> connections;

    public Broker(String brokerName) {
        this.brokerName = brokerName;
        this.msgLists = new ConcurrentHashMap<>();
        this.subscriberList = new ConcurrentHashMap<>();
        this.connections = new ConcurrentHashMap<>();
        int brokerPort = Config.hostList.get(brokerName).getPort();
        try {
            //starting broker server
            this.server = new ServerSocket(brokerPort);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

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

        logger.info("broker's line 76: someone is calling");

        Connection connection = new Connection(socket);
        return connection;
    }

    class ConnectionHandler implements Runnable{
        private Connection connection;

        public ConnectionHandler(Connection connection) {
            this.connection = connection;
        }

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
                    logger.info("broker line 102: senderName + " + senderName);
                    String type = receivedMsg.getType();
                    int startingPosition = receivedMsg.getStartingPosition();

                    if(type.equals("subscribe") && senderName.contains("consumer")){
                        String subscribedTopic = receivedMsg.getTopic();
                        logger.info("broker line 108: subscribedTopic + " + subscribedTopic);
                        ArrayList<String> subscribers = subscriberList.get(subscribedTopic);
                        if(subscribers == null){
                            subscribers = new ArrayList<>();
                        }
                        subscribers.add(senderName);
                        logger.info("broker line 113: subscriber name + " + senderName);
                        subscriberList.put(subscribedTopic, subscribers);
                    } else if(type.equals("publish") && senderName.contains("producer")) {
                        String publishedTopic = receivedMsg.getTopic();
                        logger.info("broker line 133: publishedTopic + " + publishedTopic);
                        ArrayList<MsgInfo.Msg> messages = msgLists.get(publishedTopic);
                        if(messages == null){
                            messages = new ArrayList<>();
                        }
                        messages.add(receivedMsg);
                        msgLists.put(publishedTopic, messages);
                        ArrayList<String> subscribers = subscriberList.get(publishedTopic);
                        if(subscribers != null){
                            for(String subscriber : subscribers){
                                logger.info("broker line 144: subscriber " + subscriber);
                                Connection connection = connections.get(subscriber);
                                MsgInfo.Msg requiredMsg = MsgInfo.Msg.newBuilder().setType("result").setContent(receivedMsg.getContent()).build();
                                connection.send(requiredMsg.toByteArray());
                            }
                        }
                    }

                } catch (InvalidProtocolBufferException e) {
                    e.printStackTrace();
                }
            }

        }
    }



}
