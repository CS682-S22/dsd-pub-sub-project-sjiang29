import com.google.protobuf.ByteString;
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
}
