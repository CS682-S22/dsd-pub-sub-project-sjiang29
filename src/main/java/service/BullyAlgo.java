package service;

import network.Connection;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

public class BullyAlgo {
    private int brokerId;
    private Membership membership;
    private HashMap<Integer, Connection> connections;

    public BullyAlgo(int brokerId, Membership membership, HashMap<Integer, Connection> connections) {
        this.brokerId = brokerId;
        this.membership = membership;
        this.connections = connections;
    }

    public int implementBully(){
        int newLeader;
        ArrayList<Integer> liveMembers = this.membership.getLiveMembers();

        for(int i : liveMembers){
            if (i > this.brokerId) {
                Connection connection = this.connections.get(i);
            }

        }

        return newLeader;
    }
    public void receive(){
        ArrayList<Integer> liveMembers = this.membership.getLiveMembers();

        for(int i : liveMembers){
            Connection connection = this.connections.get(i);
        }

    }

    class BullyConnectionHandler implements Runnable{
        private Connection connection;

        public BullyConnectionHandler(Connection connection) {
            this.connection = connection;
        }

        @Override
        public void run() {

        }
    }
}
