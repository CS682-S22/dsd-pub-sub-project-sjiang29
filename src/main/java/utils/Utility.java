package utils;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class Utility {
    // referenced from https://www.baeldung.com/sha-256-hashing-java
    // https://mkyong.com/java/how-to-generate-a-file-checksum-value-in-java/
    /**
     * Method to calculate a file's checkSum using SHA-256
     *
     * @param file
     * @return see method description
     */
    public static String getCheckSum(String file){
        String res = "";
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            DigestInputStream dis = new DigestInputStream(new FileInputStream(file), digest);
            while(dis.read() != -1){
                digest = dis.getMessageDigest();
            }

            StringBuilder sb = new StringBuilder();
            for(byte b : digest.digest()){
                sb.append(String.format("%02x", b));
            }
            res = sb.toString();

        } catch (NoSuchAlgorithmException | FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return res;
    }
}
