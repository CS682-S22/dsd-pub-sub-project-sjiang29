package utils;

import java.util.HashMap;

/**
 * utils.Config class: class to store App or connection's config data
 */

public class Config {

    // host1, run on "mcvm011.cs.usfca.edu" , port: 1851
    public static HostInfo host1 = new HostInfo("broker", "localhost", 1851);
    // host2, run on "mcvm012.cs.usfca.edu" , port: 1852
    public static HostInfo host2 = new HostInfo("producer1", "localhost",1852);
    // host3, run on "mcvm013.cs.usfca.edu" , port: 1853
    public static HostInfo host3 = new HostInfo("producer2", "localhost",1853);
    // host4, run on "mcvm014.cs.usfca.edu" , port: 1854
    public static HostInfo host4 = new HostInfo("producer3", "localhost",1854);
    // host5, run on "mcvm015.cs.usfca.edu" , port: 1855
    public static HostInfo host5 = new HostInfo("consumer1", "localhost",1855);
    // host6, run on "mcvm016.cs.usfca.edu" , port: 1856
    public static HostInfo host6 = new HostInfo("consumer2", "localhost",1856);
    // host7, run on "mcvm017.cs.usfca.edu" , port: 1857
    public static HostInfo host7 = new HostInfo("consumer3", "localhost",1857);

    // HashMap for looking for a host information using its name
    public static final HashMap<String, HostInfo> hostList = new HashMap<String, HostInfo>()
    {{ put(host1.getHostName(), host1); put(host2.getHostName(), host2); put(host3.getHostName(), host3);
        put(host4.getHostName(), host4); put(host5.getHostName(), host5); put(host6.getHostName(), host6);
        put(host7.getHostName(), host7);
    }};


    public static final String publishedFile1 = "/Users/sj/Desktop/Distributed Software Dev/Projects/p2/proxifier1.log";
    public static final String publishedFile2 = "/Users/sj/Desktop/Distributed Software Dev/Projects/p2/proxifier2.log";
    public static final String publishedFile3 = "/Users/sj/Desktop/Distributed Software Dev/Projects/p2/zookeeper1.log";
    // Hashmap to map producer with its published file
    public static final HashMap<String, String> producerAndFile = new HashMap<>(){{
        put(host2.getHostName(), publishedFile1); put(host3.getHostName(), publishedFile2); put(host4.getHostName(), publishedFile3);
    }};

    public static final String topic1 = "proxifier";
    public static final String topic2 = "zookeeper";
    // HashMap to map published file with its topic
    public static final HashMap<String, String> topics = new HashMap<>(){{
        put(publishedFile1, topic1); put(publishedFile2, topic1); put(publishedFile3, topic2);
    }};

    // HashMap to map consumer with its subscribed topic
    public static final HashMap<String, String> consumerAndTopic = new HashMap<>(){{
        put(host5.getHostName(), topic1); put(host6.getHostName(), topic1); put(host7.getHostName(), topic2);
    }};

    public static final String writtenFile1 = "/Users/sj/Desktop/Distributed Software Dev/Projects/p2/consumer1.txt";
    public static final String writtenFile2 = "/Users/sj/Desktop/Distributed Software Dev/Projects/p2/consumer2.txt";
    public static final String writtenFile3 = "/Users/sj/Desktop/Distributed Software Dev/Projects/p2/consumer3.txt";
    // HashMap to map consumer with its written file
    public static final HashMap<String, String> consumerAndFile = new HashMap<>(){{
        put(host5.getHostName(), writtenFile1); put(host6.getHostName(), writtenFile2); put(host7.getHostName(), writtenFile3);
    }};

    public static final int startingPosition = 0;


}
