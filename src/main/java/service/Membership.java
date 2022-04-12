package service;

import java.util.ArrayList;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class Membership {
    private ConcurrentHashMap<Integer, Boolean> members;

    public Membership() {
        this.members = new ConcurrentHashMap<>();
    }

    public void markAlive(int id){
        this.members.put(id, true);
    }
    public Set<Integer> getAllMembers() {
        return this.members.keySet();
    }

    public void markDown(int id){
        this.members.put(id, false);
    }
}
