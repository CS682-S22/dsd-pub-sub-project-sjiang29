import com.google.protobuf.ByteString;
import framework.Broker;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import proto.MsgInfo;
import utils.Config;

import java.io.*;
import java.nio.charset.StandardCharsets;

import static framework.Broker.logger;
import static utils.Utility.getCheckSum;

public class AutoTests {

    /**
     * Test protobuff handles msg correctly
     *
     */
    @Test
    public void testProtoBuff1(){
        String msg = "hello";
        byte[] msgBytes = msg.getBytes(StandardCharsets.UTF_8);
        MsgInfo.Msg sentMsg = MsgInfo.Msg.newBuilder().setContent(ByteString.copyFrom(msgBytes)).build();
        byte[] receivedBytes = sentMsg.getContent().toByteArray();
        Assertions.assertEquals(new String(msgBytes), new String(receivedBytes));
    }

    /**
     * Test protobuff handles msg correctly
     *
     */
    @Test
    public void testProtoBuff2(){
        String msg = "hello";
        byte[] msgBytes = msg.getBytes(StandardCharsets.UTF_8);
        MsgInfo.Msg sentMsg = MsgInfo.Msg.newBuilder().setContent(ByteString.copyFrom(msgBytes)).build();
        // String.valueOf doesn't work properly here
        String sentContent = String.valueOf(sentMsg.getContent());
        Assertions.assertNotEquals(msg, sentContent);
    }

    /**
     * Test method writing to a file is correct
     *
     */
    @Test
    public void testWriteToFile(){
        String sentFilePath = "sent.txt";
        String receivedFilePath = "received.txt";
        PrintWriter pw =  null;

        try (FileInputStream fileInputStream = new FileInputStream(sentFilePath);
             BufferedReader br = new BufferedReader(new InputStreamReader(fileInputStream, StandardCharsets.ISO_8859_1))) {
            String line;

            FileWriter fileWriter = new FileWriter(receivedFilePath);
            pw = new PrintWriter(fileWriter);

            while ((line = br.readLine()) != null) {
                pw.println(line);
            }
            pw.flush();

        }catch (FileNotFoundException e) {
            System.out.println("File does not exist!");
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if(pw != null){
                pw.close();
            }
            Assertions.assertEquals(getCheckSum(sentFilePath), getCheckSum(receivedFilePath));
        }
    }

    /**
     * Test broker is set correctly
     *
     */
    @Test
    public void testBroker1(){
        Broker broker = new Broker("broker");
        Assertions.assertEquals(broker.getBrokerPort(), Config.hostList.get("broker").getPort());
    }

    /**
     * Test config is set correctly
     *
     */
    @Test
    public void testConfig1(){
        Assertions.assertEquals(Config.hostList.size(), 7);
    }

    @Test
    public void testConfig2(){
        Assertions.assertEquals(Config.consumerAndFile.get("consumer1"), Config.writtenFile1);
    }

    @Test
    public void testConfig3(){
        Assertions.assertEquals(Config.consumerAndFile.get("consumer2"), Config.writtenFile2);
    }

    @Test
    public void testConfig4(){
        Assertions.assertEquals(Config.consumerAndFile.get("consumer3"), Config.writtenFile3);
    }

    @Test
    public void testConfig5(){
        Assertions.assertEquals(Config.consumerAndTopic.get("consumer1"), Config.topic1);
    }

    @Test
    public void testConfig6(){
        Assertions.assertEquals(Config.consumerAndTopic.get("consumer2"), Config.topic1);
    }

    @Test
    public void testConfig7(){
        Assertions.assertEquals(Config.consumerAndTopic.get("consumer3"), Config.topic2);
    }
}
