package service;

import java.util.ArrayList;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class Membership {
    private ConcurrentHashMap<Integer, Boolean> members;

    public Membership() {
        this.members = new ConcurrentHashMap<>();
    }

    public void addMember(int id, boolean isAlive){
        this.members.put(id, isAlive);
    }
    public Set<Integer> getAllMembers() {
        return this.members.keySet();
    }
}
