package service;

import utils.Config;
import utils.HostInfo;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;


/**
 * Membership: class to store live brokers connected with current broker
 */
public class Membership {

    private ConcurrentHashMap<Integer, Boolean> members;
    private volatile int leaderId;


    /**
     * Constructor
     */
    public Membership(){
        this.members = new ConcurrentHashMap<Integer, Boolean>();
        this.leaderId = Config.leaderId;
    }

    /**
     * Getter to get leader id
     * @return
     */
    synchronized public int getLeaderId() {
        return this.leaderId;
    }

    /**
     * Setter to get leader id
     * @param leaderId
     */
    synchronized public void setLeaderId(int leaderId) {
        this.leaderId = leaderId;
    }

    /**
     * Helper to add some live broker to members
     * @param id
     */
    public void markAlive(int id){
        this.members.put(id, true);
    }

    /**
     * Helper to return all live member
     * @return
     */
    public Set<Integer> getAllMembers() {
        return this.members.keySet();
    }

    public Set<Integer> getAllLiveBrokers(){
        Set<Integer> liveBrokers = new HashSet<>();
        for(int i : this.members.keySet()){
            if(i > 0) {
                liveBrokers.add(i);
            }
        }

        return liveBrokers;
    }

    public Set<Integer> getAllLiveLoadBalancers(){
        Set<Integer> liveLoadBalancers = new HashSet<>();
        for(int i : this.members.keySet()){
            if(i < 0) {
                liveLoadBalancers.add(i);
            }
        }

        return liveLoadBalancers;
    }
    /**
     * Helper to remove some live broker from members
     * @param id
     */
    public void markDown(int id){
        this.members.remove(id);
    }

    /**
     * Helper to print all live members
     */
    public void printLiveMembers(){
        String s = this.members.keySet().toString();
        System.out.println("live members:" + s);
    }


    /**
     * Helper to get list of followers of some broker
     * @param id
     */
    public ArrayList<Integer> getFollowers(int id){
        ArrayList<Integer> followers = new ArrayList<>();
        for(int i : this.members.keySet()){
            if(i > 0 && i < id && this.members.get(i)){
                followers.add(i);
            }
        }
        return followers;
    }
}