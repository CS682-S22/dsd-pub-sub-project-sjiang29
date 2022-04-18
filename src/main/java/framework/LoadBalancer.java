package framework;

import network.Connection;
import utils.Config;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.concurrent.ConcurrentHashMap;

public class LoadBalancer {
    private String loadBalancerName;
    private int loadBalancerPort;
    private boolean isRunning;
    private ConcurrentHashMap<String, Connection> connections;
    private ServerSocket server;

    public LoadBalancer(String loadBalancerName) {
        this.loadBalancerName = loadBalancerName;
        this.loadBalancerPort = Config.hostList.get(loadBalancerName).getPort();
        this.isRunning = true;
        this.connections = new ConcurrentHashMap<>();
        try {
            this.server = new ServerSocket(this.loadBalancerPort);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void start(){
        while(this.isRunning){
            Connection connection = Server.buildNewConnection(this.server);
            Thread connectionHandler = new Thread(new LoadBalancer.ConnectionHandler(connection));
            connectionHandler.start();
        }

    }


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

        }
    }


}
