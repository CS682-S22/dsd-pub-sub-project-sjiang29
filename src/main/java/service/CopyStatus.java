package service;

public class CopyStatus {
    private int requiredCopyNum = 0;
    private int numOfSuccessCopy = 0;

    public CopyStatus(int requiredCopyNum) {
        this.requiredCopyNum = requiredCopyNum;
        this.numOfSuccessCopy = 0;
    }

    public synchronized void incrementSuccessCopy(){
        this.numOfSuccessCopy++;
    }

    public synchronized void decrementSuccessfulCopy(){
        this.numOfSuccessCopy--;
    }

    public synchronized int getNumOfSuccessCopy(){
        return this.numOfSuccessCopy;
    }

    public void setNumOfSuccessCopy(int numOfSuccessCopy) {
        this.numOfSuccessCopy = numOfSuccessCopy;
    }

    public int getRequiredCopyNum() {
        return this.requiredCopyNum;
    }
}
