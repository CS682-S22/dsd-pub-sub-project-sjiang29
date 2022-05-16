package utils;

public class UI {

    public static void askForBrokerType(){
        System.out.println("------Please input the type of broker either 'pull' or 'push'------");
    }

    public static void askForCopyNum(){
        System.out.println("------Please input a number to indicate how many copies to duplicate'------");
    }

    public static void askForStartingPosition(){
        System.out.println("------Please input a number to indicate the starting position where you want to poll data from producer'------");
    }
}
