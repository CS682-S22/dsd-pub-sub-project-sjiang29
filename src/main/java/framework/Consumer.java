package framework;

import com.google.protobuf.InvalidProtocolBufferException;
import network.Connection;
import proto.MsgInfo;
import utils.Config;

import java.io.IOException;
import java.net.Socket;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import static framework.Broker.logger;


/**
 * Consumer class:  runnable consumer to send request to broker and consume message
 */
public class Consumer implements Runnable {
    private String leaderBrokerName;
    private String leaderBrokerAddress;
    private int leaderBrokerPort;
    int leaderBrokerId;
    private Connection leaderBrokerConnection;
    private Connection loadBalancerConnection;
    private String consumerName;
    private String topic;
    private int startingPosition;
    private BlockingQueue<MsgInfo.Msg> subscribedMsgQ;

    /**
     * Constructor

     * @param consumerName
     * @param topic
     * @param startingPosition
     */
    public Consumer(String consumerName, String topic, int startingPosition) {
        this.leaderBrokerName = "broker5";
        this.leaderBrokerId = Config.nameToId.get(this.leaderBrokerName);
        this.leaderBrokerAddress = Config.brokerList.get(leaderBrokerId).getHostAddress();
        this.leaderBrokerPort = Config.brokerList.get(leaderBrokerId).getPort();
        this.consumerName = consumerName;
        this.topic = topic;
        logger.info("consumer line 43: topic: " + this.topic);
        this.startingPosition = startingPosition;

        //String brokerAddress = Config.hostList.get(this.leaderBrokerName).getHostAddress();
        //int brokerPort = Config.hostList.get(this.leaderBrokerName).getPort();

        this.loadBalancerConnection = Server.connectToLoadBalancer(this.consumerName);
        this.subscribedMsgQ = new LinkedBlockingQueue<>();
        try {
            Socket socket = new Socket(this.leaderBrokerAddress, this.leaderBrokerPort);
            this.leaderBrokerConnection = new Connection(socket);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void updateLeaderBrokerConnection(){

        byte[] receivedBytes = this.loadBalancerConnection.receive();
        try {
            MsgInfo.Msg receivedMsg = MsgInfo.Msg.parseFrom(receivedBytes);

            if(receivedMsg.getType().equals("coordinator")){

                int newLeaderId = receivedMsg.getLeaderId();
                this.leaderBrokerId = newLeaderId;
                logger.info("consumer line 70: new leader is promoted, new leader: " + newLeaderId);
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
     * Method to send request tp broker
     * @param startingPoint
     */
    public void sendRequest(int startingPoint){
        int requiredMsgCount = 20;
        MsgInfo.Msg requestMsg = MsgInfo.Msg.newBuilder().setType("subscribe").setTopic(this.topic).setSenderName(this.consumerName)
                .setStartingPosition(startingPoint).setRequiredMsgCount(requiredMsgCount).build();
        logger.info("line 92 send request to:" + this.leaderBrokerId);
        boolean sendingStatus = this.leaderBrokerConnection.send(requestMsg.toByteArray());
        if(sendingStatus == false) {
            updateLeaderBrokerConnection();
            this.leaderBrokerConnection.send(requestMsg.toByteArray());
        }
    }

    /**
     * Method to put received msg to blocking queue, and return number of received messages
     * @return see method description
     *
     */
    public int updateBlockingQ(int startingPoint){
        int receivedMsgCount = 0;
        boolean isReceiving = true;
        while(isReceiving){
            byte[] receivedBytes = this.leaderBrokerConnection.receive();
            if (receivedBytes == null){
                logger.info("consumer line 104: reconnect");
                updateLeaderBrokerConnection();
                this.sendRequest(this.startingPosition);
                receivedBytes = this.leaderBrokerConnection.receive();
            }
            //receivedBytes = this.leaderBrokerConnection.receive();
            try {
                MsgInfo.Msg receivedMsg = MsgInfo.Msg.parseFrom(receivedBytes);
                if(receivedMsg.getType().equals("unavailable")){
                    this.sendRequest(startingPoint);
                }else if(receivedMsg.getType().contains("stop")){
                    isReceiving = false;
                }else if(receivedMsg.getType().equals("result")) {
                    logger.info("consumer line 79: received msg " + new String(receivedMsg.getContent().toByteArray()));
                    receivedMsgCount = receivedMsgCount + 1;
                    this.subscribedMsgQ.put(receivedMsg);
                }
            } catch (InvalidProtocolBufferException | InterruptedException e) {
                updateLeaderBrokerConnection();
                e.printStackTrace();
            }
        }
        return receivedMsgCount;
    }

    /**
     * Method to poll msg out of blocking queue
     * @param timeOut
     * @return see method description
     *
     */
    public MsgInfo.Msg poll(int timeOut){
        MsgInfo.Msg polledMsg = null;
        try {
            polledMsg = this.subscribedMsgQ.poll(timeOut, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return polledMsg;
    }

    /**
     * Runnable interface method
     *
     */

    public void run() {
        //int startingPoint = this.startingPosition;
        while(this.leaderBrokerConnection.isOpen() && this.startingPosition >= 0){
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            this.sendRequest(this.startingPosition);
            int receivedMsgCount = this.updateBlockingQ(this.startingPosition);
            this.startingPosition = this.startingPosition + receivedMsgCount;
        }
        if(!this.leaderBrokerConnection.isOpen()){
            logger.info("consumer line 160: reconnect");
            updateLeaderBrokerConnection();
            run();
        }


    }





    /**
     * Method to close consumer's connection to broker
     *
     */
    public void close(){
        this.leaderBrokerConnection.close();
    }
}
