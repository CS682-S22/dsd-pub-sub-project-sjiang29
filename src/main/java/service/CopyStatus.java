package service;


/**
 * CopyStatus: class to store replication status
 */
public class CopyStatus {
    private int requiredCopyNum = 0;
    private int numOfSuccessCopy = 0;

    /**
     * Constructor
     * @param requiredCopyNum
     *
     */
    public CopyStatus(int requiredCopyNum) {
        this.requiredCopyNum = requiredCopyNum;
        this.numOfSuccessCopy = 0;
    }

    /**
     * Method to increment numOfSuccessCopy
     */
    public synchronized void incrementSuccessCopy(){
        this.numOfSuccessCopy++;
    }

    /**
     * Method to decrement numOfSuccessCopy
     */
    public synchronized void decrementSuccessfulCopy(){
        this.numOfSuccessCopy--;
    }

    /**
     * Method to get numOfSuccessCopy
     * @return
     */
    public synchronized int getNumOfSuccessCopy(){
        return this.numOfSuccessCopy;
    }


    /**
     * Method to set numOfSuccessCopy
     * @param numOfSuccessCopy
     */
    public void setNumOfSuccessCopy(int numOfSuccessCopy) {
        this.numOfSuccessCopy = numOfSuccessCopy;
    }

    /**
     * Method to get requiredCopyNum
     * @return
     */
    public int getRequiredCopyNum() {
        return this.requiredCopyNum;
    }
}
