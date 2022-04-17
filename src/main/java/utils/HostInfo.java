package utils;

/**
 * utils.HostInfo Class: class for storing a host's information
 */
public class HostInfo {
    private String hostName;
    private String hostAddress;
    private int port;
    private int id;

    /**
     * Constructor.
     * @param hostName
     * @param hostAddress
     * @param port
     */
    public HostInfo(String hostName, String hostAddress, int port, int id) {
        this.hostName = hostName;
        this.hostAddress = hostAddress;
        this.port = port;
        this.id = id;
    }

    /**
     * Getter to get hostName
     * @return
     */
    public String getHostName() {
        return this.hostName;
    }

    /**
     * Getter to get hostAddress
     * @return
     */
    public String getHostAddress() {
        return this.hostAddress;
    }

    /**
     * Getter to get port
     * @return
     */
    public int getPort() {
        return this.port;
    }

    public int getId() {
        return this.id;
    }
}
