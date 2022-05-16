import framework.Broker;
import framework.Consumer;
import framework.LoadBalancer;
import framework.Producer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import proto.MsgInfo;
import utils.Config;
import utils.UI;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;

import static framework.Broker.logger;


/**
 * App class:  driver to run the application
 */
public class App {

    /**
     * Usage of app:  java -cp p2.jar App <hostName> <startingPosition>
     * HostName could be: "broker", "producer1", "producer2", "producer3", "consumer1", "consumer2", "consumer3"
     *
     */
    public static void main(String[] args){
        if (args.length != 1){
            System.out.println("Usage of the application is:  java -cp p2.jar App <hostName>");
            System.exit(1);
        }

        String hostName = args[0];
        logger.info("hostName: " + hostName);
        run(hostName);
    }

    /**
     * Helper to run the host based on their name
     * @param hostName

     */
    public static void run(String hostName){
        if(hostName.contains("broker")){
            brokerRunner(hostName);
        } else if(hostName.contains("producer")){
            producerRunner(hostName);
        } else if(hostName.contains("consumer")){
            consumerRunner(hostName);
        } else if(hostName.contains("loadBalancer")){
            dealLoadBalancer(hostName);
        }
    }

    public static void brokerRunner(String brokerName){
        UI.askForBrokerType();
        try (BufferedReader in = new BufferedReader(new InputStreamReader(System.in));){
            String line = in.readLine();
            if (line != null){
                String[] parsedArgs = line.split(" ");
                String brokerType = parsedArgs[0];
                dealBroker(brokerName, brokerType);
            } else {
                UI.askForBrokerType();
            }
        }catch(IOException ioe){
            System.out.println(ioe.getMessage());
        }
    }

    /**
     * Helper to deal broker host
     * @param brokerName
     */
    public static void dealBroker(String brokerName, String brokerType){
        Broker broker = new Broker(brokerName, brokerType);
        broker.startBroker();
    }

    public static void producerRunner(String producerName){
        UI.askForCopyNum();
        try (BufferedReader in = new BufferedReader(new InputStreamReader(System.in));){
            String line = in.readLine();
            if (line != null){
                String[] parsedArgs = line.split(" ");
                int copyNum = Integer.parseInt(parsedArgs[0]);
                dealProducer(producerName, copyNum);
            } else {
                UI.askForCopyNum();
            }
        }catch(IOException ioe){
            System.out.println(ioe.getMessage());
        }
    }

    /**
     * Helper to deal producer host
     * @param producerName
     */
    public static void dealProducer(String producerName, int copyNum){
        String file = Config.producerAndFile.get(producerName);
        logger.info("App line 64: file" + file);
        Producer producer = new Producer(producerName, copyNum);
        runProducer(producer, file, copyNum);
    }

    /**
     * Helper to let a produce read message from a log file and send it to broker
     * @param producer
     * @param file
     */
    public static void runProducer(Producer producer, String file, int copyNum){
        producer.sendCopyNum(copyNum);
        String topic = Config.topics.get(file);
        logger.info("app 76 published topic: " + topic);
        boolean sendSuccessfully;
        try (FileInputStream fileInputStream = new FileInputStream(file);
             BufferedReader br = new BufferedReader(new InputStreamReader(fileInputStream, StandardCharsets.ISO_8859_1))) {
            String line;
            while ((line = br.readLine()) != null) {
                logger.info("app 81 published line: " + line);
                byte[] data = line.getBytes(StandardCharsets.UTF_8);
                Thread.sleep(500);
                producer.send(topic, data);

                sendSuccessfully = producer.sendSuccessfully(topic, data);
                logger.info("app 95 published successfully line: " + sendSuccessfully);
                while(!sendSuccessfully){
                    producer.send(topic, data);
                    sendSuccessfully = producer.sendSuccessfully(topic, data);
                }

            }
            //producer.close();
        }catch (FileNotFoundException e) {
            System.out.println("File does not exist!");
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    public static void consumerRunner(String consumerName){
        UI.askForStartingPosition();
        try (BufferedReader in = new BufferedReader(new InputStreamReader(System.in));){
            String line = in.readLine();
            if (line != null){
                String[] parsedArgs = line.split(" ");
                int startingPos = Integer.parseInt(parsedArgs[0]);
                dealConsumer(consumerName, startingPos);
            } else {
                UI.askForStartingPosition();
            }
        }catch(IOException ioe){
            System.out.println(ioe.getMessage());
        }
    }
    /**
     * Helper to deal consumer host
     * @param consumerName
     * @param startingPosition
     */
    public static void dealConsumer(String consumerName, int startingPosition){
        String writtenFile = Config.consumerAndFile.get(consumerName);
        String subscribedTopic = Config.consumerAndTopic.get(consumerName);

        Consumer consumer = new Consumer(consumerName, subscribedTopic, startingPosition);
        Thread t1 = new Thread(consumer);
        t1.start();
        Thread t2 = new Thread(() -> saveToFile(consumer, writtenFile));
        t2.start();

        try{
            t1.join();
            t2.join();

        }catch(InterruptedException e){
            System.out.println(e);
        }
        consumer.close();
    }

    /**
     * Helper to let consumer write its subscribed message to a file
     * @param consumer
     * @param file
     */
    public static void saveToFile(Consumer consumer, String file){
        PrintWriter pw = null;
        try {
            FileWriter fileWriter = new FileWriter(file);
            pw = new PrintWriter(fileWriter);
            while(true){
                MsgInfo.Msg msg = consumer.poll(100);
                if(msg != null){
                    // String.valueOf doesn't not work
                    String line = new String(msg.getContent().toByteArray());
                    logger.info("app line 134 " + line);
                    pw.println(line);
                }
                // important to flush
                pw.flush();
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if(pw != null){
                pw.close();
            }
        }
    }

    public static void dealLoadBalancer(String loadBalancer){
        LoadBalancer lb = new LoadBalancer(loadBalancer);
        lb.start();
    }
}
