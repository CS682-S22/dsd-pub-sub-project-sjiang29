package framework;

import network.Connection;
import network.FaultInjector;
import network.LossyInjector;
import utils.Config;

import java.io.IOException;
import java.net.Socket;

public class Consumer {
    private String brokerName;
    private String consumerName;
    private Connection connection;
    private int startingPosition;

    public Consumer(String brokerName, String consumerName, Connection connection, int startingPosition) {
        this.brokerName = brokerName;
        this.consumerName = consumerName;
        this.startingPosition = startingPosition;

        String brokerAddress = Config.hostList.get(this.brokerName).getHostAddress();
        int brokerPort = Config.hostList.get(brokerName).getPort();
        try {
            Socket socket = new Socket(brokerAddress, brokerPort);
            FaultInjector fi = new LossyInjector(Config.lossRate);
            this.connection = new Connection(socket, fi);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }


}
