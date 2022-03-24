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
    private Connection connection;
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
        int brokerPort = Config.hostList.get(brokerName).getPort();
        try {
            Socket socket = new Socket(brokerAddress, brokerPort);
            this.connection = new Connection(socket);
            this.subscribedMsgQ = new LinkedBlockingQueue<>();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Method to send request tp broker
     */
    public void sendRequest(){
        MsgInfo.Msg requestMsg = MsgInfo.Msg.newBuilder().setType("subscribe").setTopic(this.topic)
                .setSenderName(this.consumerName).setStartingPosition(this.startingPosition).build();
        this.connection.send(requestMsg.toByteArray());
    }

    /**
     * Method to put received msg to blocking queue, and return number of received messages
     * @return see method description
     *
     */
    public void updateBlockingQ(){
        boolean isReceiving = true;
        while(isReceiving){
            byte[] receivedBytes = this.connection.receive();
            try {
                MsgInfo.Msg receivedMsg = MsgInfo.Msg.parseFrom(receivedBytes);
                if(receivedMsg.getType().equals("result")) {
                    logger.info("consumer line 72: received msg " + new String(receivedMsg.getContent().toByteArray()));
                    this.subscribedMsgQ.put(receivedMsg);
                }
            } catch (InvalidProtocolBufferException | InterruptedException e) {
                e.printStackTrace();
            }
        }
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
        this.sendRequest();
        this.updateBlockingQ();
    }

    /**
     * Method to close consumer's connection to broker
     *
     */
    public void close(){
        this.connection.close();
    }
}
