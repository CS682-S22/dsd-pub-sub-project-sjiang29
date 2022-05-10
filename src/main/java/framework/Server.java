package framework;

import network.Connection;
import proto.MsgInfo;
import utils.Config;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import static framework.Broker.logger;


/**
 * Server class: build static helper methods which can be used across different kinds of hosts
 */
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

    /**
     * Build connection between senderName and load balancer
     * @param senderName
     * @return connection to load balancer
     */
    public static Connection connectToLoadBalancer(String loadBalancerName, String senderName){
        //String loadBalancerName = "loadBalancer";
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

    public static void connectToLoadBalancers(ConcurrentHashMap<String, Connection> loadBalancerConnections, String senderName){
        for(String loadBalancerName : Config.loadBalancerList.keySet()){
            Connection connection = connectToLoadBalancer(loadBalancerName, senderName);
            loadBalancerConnections.put(loadBalancerName, connection);
        }
    }

    /**
     * Build data version acoording to broker's msgLists
     * @param msgLists
     * @return  a string represents data version
     */
    public static String buildDataVersion(ConcurrentHashMap<String, CopyOnWriteArrayList<MsgInfo.Msg>> msgLists){
        StringBuilder sb = new StringBuilder();
        String topic1 = Config.topic1;
        String topic2 = Config.topic2;
        int num1 = 0;
        int num2 = 0;

        if(msgLists.containsKey(topic1)){
            num1 = msgLists.get(topic1).size();
        }
        if(msgLists.containsKey(topic2)){
            num2 = msgLists.get(topic2).size();
        }

        sb.append(topic1).append(":").append(num1).append(";").append(topic2).append(":").append(num2);
        return sb.toString();
    }

    /**
     * Method to figure out each topic's count according to dataVersion string
     * @param dataVersion
     * @return  two dimensional int
     */
    public static int[] getTopicMsgCount(String dataVersion){
        int[] res = new int[2];
        ArrayList<Integer> nums = new ArrayList<>();
        String[] parts = dataVersion.split(";");
        for(String part : parts){
            String[] data = part.split(":");
            int num = Integer.parseInt(data[1]);
            nums.add(num);
        }
        res[0] = nums.get(0);
        res[1] = nums.get(1);
        return res;
    }

    /**
     * Method to figure out earliest data version according to dataVersion list
     * @param dataVersions
     * @return  a string represents earliest data version
     */
    public static String pickEarliestDataVersion(CopyOnWriteArrayList<String> dataVersions){
        String s = dataVersions.get(0);
        int[] countsOfS = Server.getTopicMsgCount(s);
        int count1 = countsOfS[0];
        int count2 = countsOfS[1];

        for(String dv : dataVersions){
            int[] nums = Server.getTopicMsgCount(dv);
            int nums1 = nums[0];
            int nums2 = nums[1];
            if(count1 >= nums1){
                count1 = nums1;
            }
            if(count2 >= nums2){
                count2 = nums2;
            }
        }
        StringBuilder sb = new StringBuilder();
        sb.append(Config.topic1).append(":").append(count1).append(";").append(Config.topic2).append(":").append(count2);
        return sb.toString();
    }



}
