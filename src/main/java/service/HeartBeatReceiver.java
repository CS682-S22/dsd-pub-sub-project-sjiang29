package service;

import com.google.protobuf.InvalidProtocolBufferException;
import network.Connection;
import proto.MsgInfo;

import java.util.Hashtable;

public class HeartBeatReceiver implements Runnable{
    private Connection connection;
    private Hashtable<Integer, Long> receivedHeartBeatTime;
    @Override
    public void run() {
        byte[] receivedBytes = this.connection.receive();
        try {
            MsgInfo.Msg receivedMsg = MsgInfo.Msg.parseFrom(receivedBytes);
            String type = receivedMsg.getType();
            if(type.equals("HeartBeat")){
                long currentTime = System.nanoTime();
                int id = receivedMsg.getSenderId();
                this.receivedHeartBeatTime.put(id, currentTime);
            }
        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
        }

    }

    private void receiveHeartBeat() {

    }
}
