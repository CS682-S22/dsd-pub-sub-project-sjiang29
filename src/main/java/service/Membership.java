package service;

import utils.Config;
import utils.HostInfo;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

public class Membership {
    //private HashMap<String, Integer> nameToIdMap;
    //private HashMap<Integer, String> idToNameMap;
    private ConcurrentHashMap<Integer, Boolean> members;
    private volatile int leaderId;


    public Membership(ConcurrentHashMap<Integer, Boolean> members, int leaderId) {
        //this.nameToIdMap = nameToIdMap;
        //this.idToNameMap = idToNameMap;
        this.members = members;
        this.leaderId = leaderId;
    }

    public Membership(){
        this.members = new ConcurrentHashMap<Integer, Boolean>();
        this.leaderId = Config.leaderId;
    }

    synchronized public int getLeaderId() {
        return this.leaderId;
    }

    synchronized public void setLeaderId(int leaderId) {
        this.leaderId = leaderId;
    }

    public void markAlive(int id){
        this.members.put(id, true);
    }
    public Set<Integer> getAllMembers() {


        return this.members.keySet();
    }


    public void markDown(int id){
        this.members.remove(id);

    }


//    public int getMaxLiveId() {
//        int res = Integer.MIN_VALUE;
//        for(int i: this.members.keySet()){
//            if(res < i && this.members.get(i)){
//                res = i;
//            }
//        }
//        return res;
//    }

    public ArrayList<Integer> getFollowers(int id){
        ArrayList<Integer> followers = new ArrayList<>();
        for(int i : this.members.keySet()){
            if(i < id){
                followers.add(i);
            }
        }
        return followers;
    }
}