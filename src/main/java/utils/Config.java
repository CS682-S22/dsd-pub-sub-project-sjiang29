package utils;

import java.util.HashMap;

/**
 * utils.Config class: class to store App or connection's config data
 */

public class Config {

    // host1, run on "mcvm011.cs.usfca.edu" , port: 1851
    public static HostInfo broker1 = new HostInfo("broker1", "mcvm011.cs.usfca.edu", 1851, 6);
    public static HostInfo broker2 = new HostInfo("broker2", "mcvm011.cs.usfca.edu", 1852, 7);
    public static HostInfo broker3 = new HostInfo("broker3", "mcvm011.cs.usfca.edu", 1853, 8);
    public static HostInfo broker4 = new HostInfo("broker4", "mcvm011.cs.usfca.edu", 1854, 9);
    public static HostInfo broker5 = new HostInfo("broker5", "mcvm011.cs.usfca.edu", 1855, 10);
    // host2, run on "mcvm012.cs.usfca.edu" , port: 1852
    public static HostInfo producer1 = new HostInfo("producer1", "mcvm012.cs.usfca.edu",1856, 0);
    public static HostInfo producer2 = new HostInfo("producer2", "mcvm012.cs.usfca.edu",1857, 0);

    public static HostInfo consumer1 = new HostInfo("consumer1", "mcvm015.cs.usfca.edu",1858, 0);
    public static HostInfo consumer2 = new HostInfo("consumer2", "mcvm015.cs.usfca.edu",1859, 0);

    public static HostInfo loadBalancer = new HostInfo("loadBalancer", "mcvm015.cs.usfca.edu",1860, 0);


    // HashMap for looking for a host information using its name
    public static final HashMap<String, HostInfo> hostList = new HashMap<String, HostInfo>()
    {{ put(broker1.getHostName(), broker1); put(broker2.getHostName(), broker2); put(broker3.getHostName(), broker3);
        put(broker4.getHostName(), broker4); put(broker5.getHostName(), broker5); put(producer1.getHostName(), producer1);
        put(producer2.getHostName(), producer2); put(consumer1.getHostName(), consumer1); put(consumer2.getHostName(), consumer2);
        put(loadBalancer.getHostName(), loadBalancer);
    }};

    //
    public static final HashMap<Integer, HostInfo> brokerList = new HashMap<Integer, HostInfo>()
    {{ put(broker1.getId(), broker1); put(broker2.getId(), broker2); put(broker3.getId(), broker3);
        put(broker4.getId(), broker4); put(broker5.getId(), broker5);
    }};

    public static final int leaderId = 10;



    //
    public static final HashMap<String, Integer> nameToId = new HashMap<String, Integer>(){{
        put("broker1", 6); put("broker2", 7);put("broker1", 8);put("broker1", 9);put("broker1", 10);
    }};
    public static final String publishedFile1 = "proxifier1.log";
    public static final String publishedFile2 = "proxifier2.log";
    public static final String publishedFile3 = "zookeeper1.log";
    // Hashmap to map producer with its published file
    public static final HashMap<String, String> producerAndFile = new HashMap<>(){{
        put(producer1.getHostName(), publishedFile1); put(producer2.getHostName(), publishedFile3);
    }};

    public static final String topic1 = "proxifier";
    public static final String topic2 = "zookeeper";
    // HashMap to map published file with its topic
    public static final HashMap<String, String> topics = new HashMap<>(){{
        put(publishedFile1, topic1); put(publishedFile2, topic1); put(publishedFile3, topic2);
    }};

    // HashMap to map consumer with its subscribed topic
    public static final HashMap<String, String> consumerAndTopic = new HashMap<>(){{
        put(consumer1.getHostName(), topic1); put(consumer2.getHostName(), topic2);
    }};

    public static final String writtenFile1 = "consumer1.txt";
    public static final String writtenFile2 = "consumer2.txt";
    public static final String writtenFile3 = "consumer3.txt";
    // HashMap to map consumer with its written file
    public static final HashMap<String, String> consumerAndFile = new HashMap<>(){{
        put(consumer1.getHostName(), writtenFile1); put(consumer2.getHostName(), writtenFile2);
    }};

    public static final int startingPosition = 0;


}
