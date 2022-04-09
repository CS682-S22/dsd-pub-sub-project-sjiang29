package service;

import java.util.Hashtable;
import java.util.Set;

public class HeartBeatChecker implements Runnable{
    private Hashtable<Integer, Long> heartBeatReceivedTimes;
    private long timeoutNanos;

    public HeartBeatChecker(Hashtable<Integer, Long> heartBeatReceivedTimes, long timeoutNanos ) {
        this.heartBeatReceivedTimes = heartBeatReceivedTimes;
        this.timeoutNanos = timeoutNanos;
    }

    @Override
    public void run() {
        Long now = System.nanoTime();
        Set<Integer> brokerIds = this.heartBeatReceivedTimes.keySet();
        for (Integer brokerId : brokerIds) {
            Long lastHeartbeatReceivedTime = this.heartBeatReceivedTimes.get(brokerId);
            Long timeSinceLastHeartbeat = now - lastHeartbeatReceivedTime;
            if (timeSinceLastHeartbeat >= timeoutNanos) {
                //markDown(serverId);
            }
        }
    }
}
