package framework;

import network.Connection;
import proto.MsgInfo;
import utils.Config;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

import static framework.Broker.logger;

public class Server {

    /**
     * Listens to new socket connection, return corresponding connection according to value of delay and lossRate
     * @return see method description
     */
    public static Connection buildNewConnection(ServerSocket server) {
        Socket socket = null;
        try {
            socket = server.accept();
        } catch (IOException e) {
            e.printStackTrace();
        }

        logger.info("server's line 20: someone is calling");
        Connection connection = new Connection(socket);
        return connection;
    }

    public static Connection connectToLoadBalancer(String senderName){
        String loadBalancerName = "loadBalancer";
        String loadBalancerAddress = Config.hostList.get(loadBalancerName).getHostAddress();
        int loadBalancerPort = Config.hostList.get(loadBalancerName).getPort();
        Connection connection = null;
        try {
            Socket socket = new Socket(loadBalancerAddress, loadBalancerPort);
            connection = new Connection(socket);
            MsgInfo.Msg greetingMsg = MsgInfo.Msg.newBuilder().setType("greeting").setSenderName(senderName).build();
            connection.send(greetingMsg.toByteArray());

        } catch (IOException e) {
            e.printStackTrace();
        }
        return connection;
    }
}
