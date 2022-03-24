package framework;

import com.google.protobuf.ByteString;
import network.Connection;
import proto.MsgInfo;
import utils.Config;

import java.io.IOException;
import java.net.Socket;

import static framework.Broker.logger;
/**
 * Producer class: class to publish message to broker
 *
 */
public class Producer {
    private String brokerName;
    private String producerName;
    private Connection connection;
    private int msgId;

    /**
     * Constructor
     * @param brokerName
     * @param producerName
     *
     */
    public Producer(String brokerName, String producerName) {
        this.msgId = 1;
        this.brokerName = brokerName;
        this.producerName = producerName;
        String brokerAddress = Config.hostList.get(this.brokerName).getHostAddress();
        int brokerPort = Config.hostList.get(brokerName).getPort();
        try {
            Socket socket = new Socket(brokerAddress, brokerPort);
            this.connection = new Connection(socket);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Method to send message of some topic to a broker
     * @param topic
     * @param data
     *
     */
    public void send(String topic, byte[] data){
        MsgInfo.Msg sentMsg = MsgInfo.Msg.newBuilder().setTopic(topic).setType("publish").setContent(ByteString.copyFrom(data)).setId(this.msgId++).setSenderName(this.producerName).build();
        this.connection.send(sentMsg.toByteArray());

    }

    /**
     * Method to close the connection to a broker
     *
     */
    public void close(){
        this.connection.close();
    }
}
