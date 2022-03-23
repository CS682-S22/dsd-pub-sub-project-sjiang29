import framework.Broker;
import framework.Consumer;
import framework.Producer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import proto.MsgInfo;
import utils.Config;

import java.io.*;
import java.nio.charset.StandardCharsets;
import static framework.Broker.logger;

public class App {

    public static void main(String[] args){
        if (args.length != 1){
            System.out.println("Usage of the application is: java App <hostName> ");
            System.exit(1);
        }

        String hostName = args[0];
        logger.info("hostName: " + hostName);
        run(hostName);

    }

    public static void run(String hostName){
        if(hostName.equals("broker")){
            dealBroker(hostName);
        } else if(hostName.contains("producer")){
            dealProducer(hostName);
        } else if(hostName.contains("consumer")){
            dealConsumer(hostName);
        }
    }

    public static void dealBroker(String brokerName){
        Broker broker = new Broker(brokerName);
        broker.startBroker();
    }

    public static void dealProducer(String producerName){
        String file = Config.producerAndFile.get(producerName);
        logger.info("App line 45: file" + file);
        Producer producer = new Producer("broker", producerName);
        runProducer(producer, file);
    }

    public static void runProducer(Producer producer, String file){
        try (FileInputStream fileInputStream = new FileInputStream(file);
             BufferedReader br = new BufferedReader(new InputStreamReader(fileInputStream, StandardCharsets.ISO_8859_1))) {
            String line;

            while ((line = br.readLine()) != null) {
                logger.info("app 56 published line: " + line);
                byte[] data = line.getBytes(StandardCharsets.UTF_8);
                String topic = Config.topics.get(file);
                producer.send(topic, data);
            }
            producer.close();

        }catch (FileNotFoundException e) {
            System.out.println("File does not exist!");
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public static void dealConsumer(String consumerName){
        String writtenFile = Config.consumerAndFile.get(consumerName);
        String subscribedTopic = Config.consumerAndTopic.get(consumerName);
        int startingPosition = Config.startingPosition;

        Consumer consumer = new Consumer("broker", consumerName, subscribedTopic, startingPosition);
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
                    logger.info("app line 112 " + line);
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
}
