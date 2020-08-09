package poc.gossip.model;

import java.net.Socket;

public class Connection {
    private final String ip;
    private final int port;
    private final Socket socket;

    private boolean isActive;

    public Connection(String ip, int port, Socket socket) {
        this.ip = ip;
        this.port = port;
        this.socket = socket;
    }

    public String getIp() {
        return ip;
    }

    public int getPort() {
        return port;
    }

    public boolean isActive() {
        return isActive;
    }

    public void setActive(boolean active) {
        isActive = active;
    }

    public String name() {
        return String.format("%s:%s", this.ip, this.port);
    }

    public Socket getSocket() {
        return socket;
    }

    @Override
    public String toString() {
        return "(" + ip + ":" + port + ")";
    }
}
