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
    private CopyOnWriteArrayList<Integer> members;
    private int leaderId;


    public Membership(CopyOnWriteArrayList<Integer> members, int leaderId) {
        //this.nameToIdMap = nameToIdMap;
        //this.idToNameMap = idToNameMap;
        this.members = members;
        this.leaderId = leaderId;
    }

    public Membership(){
        this.members = new CopyOnWriteArrayList<>();
        this.leaderId = Config.leaderId;
    }

    synchronized public int getLeaderId() {
        return this.leaderId;
    }

    synchronized public void setLeaderId(int leaderId) {
        this.leaderId = leaderId;
    }

    public void markAlive(int id){
        this.members.add(id);
    }
    public CopyOnWriteArrayList<Integer> getAllMembers() {
        return this.members;
    }
    public CopyOnWriteArrayList<Integer> getLiveMembers() {
        return this.members;
    }

    public void markDown(int id){
        for(int i = 0; i < this.members.size(); i++){
            if(id == this.members.get(i)){
                this.members.remove(i);
            }
        }

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
        for(int i : this.members){
            if(i < id){
                followers.add(i);
            }
        }
        return followers;
    }
}