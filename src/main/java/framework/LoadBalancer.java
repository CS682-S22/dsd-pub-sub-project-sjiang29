package framework;

import com.google.protobuf.InvalidProtocolBufferException;
import network.Connection;
import proto.MsgInfo;
import utils.Config;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.concurrent.ConcurrentHashMap;

import static framework.Broker.logger;

/**
 * LoadBalancer class: class to send coordinator msg to producer and consumer
 */
public class LoadBalancer {
    private String loadBalancerName;
    private int loadBalancerPort;
    private boolean isRunning;
    private int newLeaderId;
    private ConcurrentHashMap<String, Connection> connections;
    private ServerSocket server;

    /**
     * Load balancer Constructor
     * @param loadBalancerName
     */
    public LoadBalancer(String loadBalancerName) {
        this.loadBalancerName = loadBalancerName;
        this.loadBalancerPort = Config.hostList.get(loadBalancerName).getPort();
        this.isRunning = true;
        this.newLeaderId = 0;
        this.connections = new ConcurrentHashMap<>();
        try {
            logger.info("load balancer line 29: load balancer starts");
            this.server = new ServerSocket(this.loadBalancerPort);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Method to start load balancer
     */
    public void start(){
        while(this.isRunning){
            Connection connection = Server.buildNewConnection(this.server);
            Thread connectionHandler = new Thread(new LoadBalancer.ConnectionHandler(connection));
            connectionHandler.start();
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

        @Override
        public void run() {
            while(isRunning){
                byte[] receivedBytes = this.connection.receive();
                try {
                    MsgInfo.Msg receivedMsg = MsgInfo.Msg.parseFrom(receivedBytes);
                    String senderName = receivedMsg.getSenderName();
                    connections.put(senderName, this.connection);
                    logger.info("load balancer line 62: senderName + " + senderName + " type " + receivedMsg.getType());
                    String type = receivedMsg.getType();
                    if(isBrokerReq(type, senderName)) {
                        newLeaderId = receivedMsg.getLeaderId();
                        logger.info("receive coordinator from " + senderName + " new leader " + newLeaderId);
                        notifyAllHosts();
                    }
                } catch (InvalidProtocolBufferException e) {
                    e.printStackTrace();
                }
            }

        }

        /**
         * Helper to check if an incoming msg is from broker and also is a coordinator msg
         * @param senderName
         * @param type
         */
        private boolean isBrokerReq(String type, String senderName){
            return type.equals("coordinator") && senderName.contains("broker");
        }

        /**
         * Helper to send coordinator msg to producer and consumer
         */
        private void notifyAllHosts(){
            MsgInfo.Msg coordinatorMsg = MsgInfo.Msg.newBuilder().setType("coordinator").setSenderName(loadBalancerName).
                    setLeaderId(newLeaderId).build();
            for(String receiver : connections.keySet()){
                if(receiver.contains("producer") || receiver.contains("consumer")){
                    logger.info("load balancer line 89: send coordinator msg to + " + receiver);
                    Connection connection = connections.get(receiver);
                    connection.send(coordinatorMsg.toByteArray());
                }

            }
        }
    }


}
