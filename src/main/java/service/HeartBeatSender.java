package service;

import com.google.protobuf.ByteString;
import network.Connection;
import proto.MsgInfo;

import static framework.Broker.logger;

public class HeartBeatSender implements Runnable{

    private Connection connection;
    private int senderId;
    private String senderName;

    public HeartBeatSender(Connection connection, int senderId, String senderName) {
        this.connection = connection;
        this.senderId = senderId;
        this.senderName = senderName;
    }

    @Override
    public void run() {
        logger.info("hb sender line 23: sending hb from " + this.senderId);
        MsgInfo.Msg heartBeatMsg = MsgInfo.Msg.newBuilder().setType("HeartBeat")
                .setSenderName(this.senderName).setSenderId(this.senderId).build();
        this.connection.send(heartBeatMsg.toByteArray());
    }
}
