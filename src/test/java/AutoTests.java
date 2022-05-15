import com.google.protobuf.ByteString;
import framework.Broker;
import framework.Producer;
import framework.Server;
import network.Connection;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import proto.MsgInfo;
import service.BullyAlgo;
import service.HeartBeatChecker;
import service.Membership;
import utils.Config;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Hashtable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import static utils.Utility.getCheckSum;

public class AutoTests {
    @Test
    public void testHeartBeatChecker1(){
        String hostBrokerName = "broker5";
        ConcurrentHashMap<Integer, Long> heartBeatReceivedTimes = new ConcurrentHashMap<>();
        long timeoutNanos = 3000000000L;
        Membership membership = new Membership();
        membership.setLeaderId(Config.leaderId);
        membership.markAlive(9);
        membership.markAlive(8);
        membership.markAlive(7);
        membership.markAlive(6);
        ConcurrentHashMap<String, Connection> brokerConnections = new ConcurrentHashMap<>();
        ConcurrentHashMap<String, CopyOnWriteArrayList<MsgInfo.Msg>> msgLists = new ConcurrentHashMap<>();
        msgLists.put(Config.topic1, new CopyOnWriteArrayList<>());
        msgLists.put(Config.topic2, new CopyOnWriteArrayList<>());
        ConcurrentHashMap<String, Connection> loadBalancerConnections = new ConcurrentHashMap<>();
        Connection connectionToLoadBalancer;
        try {
            ServerSocket server = new ServerSocket(8000);
            Socket s1 = new Socket("localhost", 8000);
            Socket s2 = new Socket("localhost", 8000);
            Socket s3 = new Socket("localhost", 8000);
            Socket s4 = new Socket("localhost", 8000);
            Socket s5 = new Socket("localhost", 8000);
            brokerConnections.put("broker4", new Connection(s4));
            brokerConnections.put("broker3", new Connection(s3));
            brokerConnections.put("broker2", new Connection(s2));
            brokerConnections.put("broker1", new Connection(s1));
            connectionToLoadBalancer = new Connection(s5);
            loadBalancerConnections.put("loadBalancer", connectionToLoadBalancer);
            heartBeatReceivedTimes.put(6, 30000000000L);
            heartBeatReceivedTimes.put(7, 30000000000L);
            heartBeatReceivedTimes.put(8, 30000000000L);
            heartBeatReceivedTimes.put(9, 30000000000L);
            HeartBeatChecker hbc = new HeartBeatChecker(hostBrokerName, heartBeatReceivedTimes, timeoutNanos,
                    membership, brokerConnections, loadBalancerConnections, msgLists);
            hbc.run();
            int leader = membership.getLeaderId();
            Assertions.assertEquals(leader, 10);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testHeartBeatChecker2(){
        String hostBrokerName = "broker4";
        ConcurrentHashMap<Integer, Long> heartBeatReceivedTimes = new ConcurrentHashMap<>();
        long timeoutNanos = 3000000000L;
        Membership membership = new Membership();
        membership.setLeaderId(Config.leaderId);
        membership.markAlive(10);
        membership.markAlive(8);
        membership.markAlive(7);
        membership.markAlive(6);
        ConcurrentHashMap<String, Connection> brokerConnections = new ConcurrentHashMap<>();
        ConcurrentHashMap<String, CopyOnWriteArrayList<MsgInfo.Msg>> msgLists = new ConcurrentHashMap<>();
        msgLists.put(Config.topic1, new CopyOnWriteArrayList<>());
        msgLists.put(Config.topic2, new CopyOnWriteArrayList<>());
        ConcurrentHashMap<String, Connection> loadBalancerConnections = new ConcurrentHashMap<>();
        Connection connectionToLoadBalancer;
        try {
            ServerSocket server = new ServerSocket(8000);
            Socket s1 = new Socket("localhost", 8000);
            Socket s2 = new Socket("localhost", 8000);
            Socket s3 = new Socket("localhost", 8000);
            Socket s4 = new Socket("localhost", 8000);
            Socket s5 = new Socket("localhost", 8000);
            brokerConnections.put("broker5", new Connection(s4));
            brokerConnections.put("broker3", new Connection(s3));
            brokerConnections.put("broker2", new Connection(s2));
            brokerConnections.put("broker1", new Connection(s1));
            connectionToLoadBalancer = new Connection(s5);
            loadBalancerConnections.put("loadBalancer", connectionToLoadBalancer);
            heartBeatReceivedTimes.put(6, 30000000000L);
            heartBeatReceivedTimes.put(7, 30000000000L);
            heartBeatReceivedTimes.put(8, 30000000000L);
            heartBeatReceivedTimes.put(10, 30000000000L);
            HeartBeatChecker hbc = new HeartBeatChecker(hostBrokerName, heartBeatReceivedTimes, timeoutNanos,
                    membership, brokerConnections, loadBalancerConnections, msgLists);
            hbc.run();
            int leader = membership.getLeaderId();
            Assertions.assertEquals(leader, 9);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    @Test
    public void testBully1() {
        try {
            ServerSocket server = new ServerSocket(8000);
            Socket s1 = new Socket("localhost", 8000);
            Socket s2 = new Socket("localhost", 8000);
            Socket s3 = new Socket("localhost", 8000);
            Membership membership = new Membership();
            membership.setLeaderId(Config.leaderId);
            membership.markAlive(9);
            membership.markAlive(8);
            String hostBrokerName = "broker5";
            ConcurrentHashMap<String, Connection> brokerConnections = new ConcurrentHashMap<>();
            ConcurrentHashMap<String, Connection> loadBalancerConnections = new ConcurrentHashMap<>();

            brokerConnections.put("broker4", new Connection(s1));
            brokerConnections.put("broker3", new Connection(s2));
            Connection connectionToLoadBalancer = new Connection(s3);
            loadBalancerConnections.put("loadBalancer", connectionToLoadBalancer);
            ConcurrentHashMap<String, CopyOnWriteArrayList<MsgInfo.Msg>> msgLists = new ConcurrentHashMap<>();
            msgLists.put(Config.topic1, new CopyOnWriteArrayList<>());
            msgLists.put(Config.topic2, new CopyOnWriteArrayList<>());
            int newLeader = BullyAlgo.sendBullyReq(membership,hostBrokerName,brokerConnections, loadBalancerConnections, msgLists);
            Assertions.assertEquals(newLeader, 10);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testBully2() {
        try {
            ServerSocket server = new ServerSocket(8000);
            Socket s1 = new Socket("localhost", 8000);
            Socket s2 = new Socket("localhost", 8000);
            Socket s3 = new Socket("localhost", 8000);
            Membership membership = new Membership();
            membership.setLeaderId(7);
            membership.markAlive(9);
            membership.markAlive(8);
            String hostBrokerName = "broker2";
            ConcurrentHashMap<String, Connection> brokerConnections = new ConcurrentHashMap<>();
            ConcurrentHashMap<String, Connection> loadBalancerConnections = new ConcurrentHashMap<>();

            brokerConnections.put("broker4", new Connection(s1));
            brokerConnections.put("broker3", new Connection(s2));
            Connection connectionToLoadBalancer = new Connection(s3);
            loadBalancerConnections.put("loadBalancer", connectionToLoadBalancer);
            ConcurrentHashMap<String, CopyOnWriteArrayList<MsgInfo.Msg>> msgLists = new ConcurrentHashMap<>();
            msgLists.put(Config.topic1, new CopyOnWriteArrayList<>());
            msgLists.put(Config.topic2, new CopyOnWriteArrayList<>());

            int newLeader = BullyAlgo.sendBullyReq(membership,hostBrokerName,brokerConnections, loadBalancerConnections, msgLists);
            Assertions.assertEquals(newLeader, -1);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testMembership1(){
        Membership membership = new Membership();
        membership.markAlive(1);
        membership.markAlive(2);
        membership.markDown(1);
        int members = membership.getAllMembers().size();
        Assertions.assertEquals(members, 1);
    }

    @Test
    public void testMembership2(){
        Membership membership = new Membership();
        membership.markAlive(1);
        membership.markAlive(2);
        membership.markAlive(3);
        membership.markAlive(4);
        membership.markAlive(6);
        int followers = membership.getFollowers(4).size();
        Assertions.assertEquals(followers, 3);
    }

    @Test
    public void checkBuildReplyToNewLeader1(){
        ConcurrentHashMap<String, CopyOnWriteArrayList<MsgInfo.Msg>> msgLists = new ConcurrentHashMap<>();
        String s = Server.buildDataVersion(msgLists);
        String predicted = Config.topic1 + ":" + "0" + ";" + Config.topic2 + ":" + "0";
        Assertions.assertEquals(s, predicted);
    }

    @Test
    public void checkBuildReplyToNewLeader2(){
        MsgInfo.Msg msg1 = MsgInfo.Msg.newBuilder().setType("test").build();
        MsgInfo.Msg msg2 = MsgInfo.Msg.newBuilder().setType("test").build();
        ConcurrentHashMap<String, CopyOnWriteArrayList<MsgInfo.Msg>> msgLists = new ConcurrentHashMap<>();
        CopyOnWriteArrayList<MsgInfo.Msg> l1 = new CopyOnWriteArrayList<>();
        CopyOnWriteArrayList<MsgInfo.Msg> l2 = new CopyOnWriteArrayList<>();
        l1.add(msg1);
        l2.add(msg1);
        l2.add(msg2);
        msgLists.put(Config.topic1, l1);
        msgLists.put(Config.topic2, l2);
        String s = Server.buildDataVersion(msgLists);
        String predicted = Config.topic1 + ":" + "1" + ";" + Config.topic2 + ":" + "2";
        Assertions.assertEquals(s, predicted);
    }

    @Test
    public void testCheckGetTopicNum1(){
        String s = Config.topic1 + ":" + "1" + ";" + Config.topic2 + ":" + "2";
        int[] res = Server.getTopicMsgCount(s);
        Assertions.assertEquals(1, res[0]);
    }

    @Test
    public void testCheckGetTopicNum2(){
        String s = Config.topic1 + ":" + "1" + ";" + Config.topic2 + ":" + "9";
        int[] res = Server.getTopicMsgCount(s);
        Assertions.assertEquals(9, res[1]);
    }

    @Test
    public void pickEarliestDataVersion(){
        String broker1 = "broker1";
        String broker2 = "broker2";
        String s1 = Config.topic1 + ":" + "1" + ";" + Config.topic2 + ":" + "9";
        String s2 = Config.topic1 + ":" + "10" + ";" + Config.topic2 + ":" + "2";
        ConcurrentHashMap<String, String> dvs = new ConcurrentHashMap<>();
        dvs.put(broker1, s1);
        dvs.put(broker2, s2);

        String res = Server.pickEarliestDataVersion(dvs, broker1).getDataVersion();
        String predicted = Config.topic1 + ":" + "1" + ";" + Config.topic2 + ":" + "2";
        Assertions.assertEquals(res, predicted);


    }

    @Test
    public void pickLatestDataVersion(){
        String broker1 = "broker1";
        String broker2 = "broker2";
        String s1 = Config.topic1 + ":" + "1" + ";" + Config.topic2 + ":" + "9";
        String s2 = Config.topic1 + ":" + "10" + ";" + Config.topic2 + ":" + "2";
        ConcurrentHashMap<String, String> dvs = new ConcurrentHashMap<>();
        dvs.put(broker1, s1);
        dvs.put(broker2, s2);

        String res = Server.pickLatestDataVersion(dvs, broker1).getDataVersion();
        String topic2Owner = Server.pickLatestDataVersion(dvs, broker1).getTopic2Owner();
        String predicted = Config.topic1 + ":" + "10" + ";" + Config.topic2 + ":" + "9";
        Assertions.assertEquals(res, predicted);
        Assertions.assertEquals(broker1, topic2Owner);
    }




}
