package service;

import com.google.protobuf.ByteString;
import network.Connection;
import proto.MsgInfo;

import static framework.Broker.logger;

/**
 * HeartBeatSender: runnable class to send heart beat message
 */
public class HeartBeatSender implements Runnable{

    private Connection connection;
    private int senderId;
    private String senderName;

    /**
     * Constructor
     * @param senderName
     * @param connection
     * @param senderId
     */
    public HeartBeatSender(Connection connection, int senderId, String senderName) {
        this.connection = connection;
        this.senderId = senderId;
        this.senderName = senderName;
    }


    /**
     * Runnable interface method
     */
    @Override
    public void run() {
        MsgInfo.Msg heartBeatMsg = MsgInfo.Msg.newBuilder().setType("HeartBeat")
                .setSenderName(this.senderName).setSenderId(this.senderId).build();
        this.connection.send(heartBeatMsg.toByteArray());
        logger.info("hb sender line 27: sending hb from " + this.senderId);
    }
}
