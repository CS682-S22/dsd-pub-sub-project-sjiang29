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
public class Consumer implements Runnable{
    private String brokerName;
    private String consumerName;
    private Connection leaderBrokerConnection;
    private Connection loadBalancerConnection;
    private String topic;
    private int startingPosition;
    private BlockingQueue<MsgInfo.Msg> subscribedMsgQ;

    /**
     * Constructor
     * @param brokerName
     * @param consumerName
     * @param topic
     * @param startingPosition
     */
    public Consumer(String brokerName, String consumerName, String topic, int startingPosition) {
        this.brokerName = brokerName;
        this.consumerName = consumerName;
        this.topic = topic;
        this.startingPosition = startingPosition;

        String brokerAddress = Config.hostList.get(this.brokerName).getHostAddress();
        int brokerPort = Config.hostList.get(this.brokerName).getPort();

        String loadBalancerAddress = Config.hostList.get("loadBalancer").getHostAddress();
        int loadBalancerPort = Config.hostList.get("loadBalancer").getPort();
        try {
            Socket socket1 = new Socket(brokerAddress, brokerPort);
            this.leaderBrokerConnection = new Connection(socket1);

            Socket socket2 = new Socket(loadBalancerAddress, loadBalancerPort);
            this.loadBalancerConnection = new Connection(socket2);
            this.subscribedMsgQ = new LinkedBlockingQueue<>();
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
                this.brokerName = Config.brokerList.get(newLeaderId).getHostName();
                String leaderBrokerAddress = Config.brokerList.get(newLeaderId).getHostAddress();
                int leaderBrokerPort = Config.brokerList.get(newLeaderId).getPort();
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
        Thread t = new Thread(() -> updateLeaderBrokerConnection());
        t.start();
        int requiredMsgCount = 20;
        MsgInfo.Msg requestMsg = MsgInfo.Msg.newBuilder().setType("subscribe").setTopic(this.topic).setSenderName(this.consumerName)
                .setStartingPosition(startingPoint).setRequiredMsgCount(requiredMsgCount).build();
        this.leaderBrokerConnection.send(requestMsg.toByteArray());
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
    @Override
    public void run() {
        int startingPoint = this.startingPosition;
        while(this.leaderBrokerConnection.isOpen() && startingPoint >= 0){
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            this.sendRequest(startingPoint);
            int receivedMsgCount = this.updateBlockingQ(startingPoint);
            startingPoint = startingPoint + receivedMsgCount;
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
