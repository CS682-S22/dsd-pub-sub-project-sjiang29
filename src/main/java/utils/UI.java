package utils;

/**
 * UI class: class to deal UI print.
 */
public class UI {

    /**
     * UI print for asking for broker type from user
     */
    public static void askForBrokerType(){
        System.out.println("------Please input the type of broker either 'pull' or 'push'------");
    }

    /**
     * UI print for asking for producer's number of copies from user
     */
    public static void askForCopyNum(){
        System.out.println("------Please input a number to indicate how many copies to duplicate'------");
    }

    /**
     * UI print for asking for consumer's starting position from user
     */
    public static void askForStartingPosition(){
        System.out.println("------Please input a number to indicate the starting position where you want to poll data from producer'------");
    }
}
