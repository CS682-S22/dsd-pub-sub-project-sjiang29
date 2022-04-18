package service;

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
    private int leaderId;


    public Membership(
                      ConcurrentHashMap<Integer, Boolean> members, int leaderId) {
        //this.nameToIdMap = nameToIdMap;
        //this.idToNameMap = idToNameMap;
        this.members = members;
        this.leaderId = leaderId;
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
    public ArrayList<Integer> getLiveMembers() {
        ArrayList<Integer> liveMembers = new ArrayList<>();
        for(int i : this.members.keySet()){
            if(this.members.get(i)){
                liveMembers.add(i);
            }
        }
        return liveMembers;
    }

    public void markDown(int id){
        this.members.put(id, false);
    }


    public int getMaxLiveId() {
        int res = Integer.MIN_VALUE;
        for(int i: this.members.keySet()){
            if(res < i && this.members.get(i)){
                res = i;
            }
        }
        return res;
    }

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