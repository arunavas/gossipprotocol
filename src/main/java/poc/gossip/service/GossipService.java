package poc.gossip.service;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import poc.gossip.model.Connection;
import poc.gossip.model.HealthDetail;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.security.SecureRandom;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class GossipService {
    /**
     * TODO(s)
     *   1. when a service gets down, the send method throws exception, remove the connection after a configurable number of errors.
     *   2. merge healthMap & connMap - no need to have two separate maps
     *   3. While inactivating a peer (after not receiving update from it for last x time)
     *      it should also see for other peer's status about it instead of only depending on self
     *   4. When updating a peer as inactive, also reset the version.
     *   5. When incoming messages validate authenticity.
     *   6. Ensure randomness while sending gossips
     */

    private Map<String, HealthDetail> healthMap = new ConcurrentHashMap<>();
    private Map<String, Connection> connMap = new HashMap<>();
    private List<String> connNames = new ArrayList<>();

    private HealthDetail selfHealth;

    private long inactiveTimePeriod = 3000L;
    private int gossipShareCount = 3;

    private Random random = new SecureRandom();

    private int count = 0;

    public GossipService(String name, int port) {
        Timer timer = new Timer("health-service-checker");
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                monitorHealth();
            }
        }, 1000, 1700);
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                shareHealth();
            }
        }, 0, 1000);
        selfHealth = new HealthDetail(name, port, System.currentTimeMillis());
    }

    public void register(Connection conn, boolean broadcast) {
        System.out.println("RegConnection: " + conn);
        if (broadcast) {
            try {
                List<Connection> conns = new ArrayList<>(connMap.values());
                DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(conn.getSocket().getOutputStream()));
                dos.writeUTF("CONNS: " + toConnsJson(conns));
                dos.flush();

                conns.clear();
                conns.add(conn);
                String json = "CONNS: " + toConnsJson(conns);
                connMap.values().forEach(c -> {
                    try {
                        DataOutputStream d = new DataOutputStream(new BufferedOutputStream(c.getSocket().getOutputStream()));
                        d.writeUTF("CONNS: " + json);
                        d.flush();
                    } catch (IOException ex) {
                        System.err.println("Error sending new conn update to " + c.name());
                    }
                });
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }

        connMap.put(conn.name(), conn);
        if (!connNames.contains(conn.name())) {
            connNames.add(conn.name());
        }

        System.out.println("Registered new service: " + conn.name() + "!");
    }

    public void remove(Socket socket) {
        Connection conn = new Connection(socket.getInetAddress().getHostAddress(), socket.getPort(), socket);
        connMap.remove(conn.name());
        connNames.remove(conn.name());
        sendRemoval(conn);
        System.out.println("Removed service: " + conn.name() + "!");
    }

    //Obsolete - not needed as this can be detected eventually from the health messages
    public void updateRemove(String name) {
        Connection conn = connMap.get(name);
        if (conn != null) {
            connMap.remove(name);
            connNames.remove(name);
            sendRemoval(conn);
            System.out.println("Removed service: " + name + "!");
        }
    }

    public void updateConnection(List<Connection> conns) {
        conns.forEach(c -> {
            connMap.put(c.name(), c);
            if (!connNames.contains(c.name())) {
                connNames.add(c.name());
            }
        });
        System.out.println("ConnectionMap: " + connMap);
    }

    public void updateHealth(List<HealthDetail> details) {
        details.forEach(this::update);
        selfHealth.setLastUpdateTime(System.currentTimeMillis());
        selfHealth.setActive(true);
        count++;
        if (count % 10 == 0) {
            System.out.println("Received Health updates for " + details.stream()
                    .map(hd -> String.valueOf(hd.getPort())).collect(Collectors.joining(",")));
        }
    }

    private void update(HealthDetail detail) {
        if (healthMap.containsKey(detail.name())) {
            healthMap.get(detail.name()).update(detail);
        } else if (!selfHealth.equals(detail)) {
            healthMap.put(detail.name(), detail);
        }
    }

    private volatile long cntr = 0;
    private void monitorHealth() {
        if (selfHealth.isActive()) {
            healthMap.forEach((k, v) -> {
                if (v.isActive() && (System.currentTimeMillis() - v.getLastUpdateTime()) > inactiveTimePeriod) {
                    v.setActive(false);
                    System.out.println(v.name() + " marked as INACTIVE! Last Received: " + v.getLastUpdateTime() + " i.e. " + (System.currentTimeMillis() - v.getLastUpdateTime()) + " millis ago!");
                }
            });
            if ((System.currentTimeMillis() - selfHealth.getLastUpdateTime()) > inactiveTimePeriod) {
                selfHealth.setActive(false);
                System.out.println("SELF marked as INACTIVE! Las Received: " + selfHealth.getLastUpdateTime() + " i.e. " + (System.currentTimeMillis() - selfHealth.getLastUpdateTime()) + " millis ago!");
            }
        }

        cntr++;
        if (cntr % 10 == 0) {
            System.out.println("Health Map: " + healthMap + "\nConn Map: " + connMap);
        }
    }

    private void shareHealth() {
        if (gossipShareCount > connMap.size()) {
            connMap.forEach((k, v) -> {
                if (!healthMap.containsKey(k) || healthMap.get(k).isActive()) {
                    send(v);
                }
            });
        } else {
            for (int i = 0; i < gossipShareCount; ) {
                int idx = Math.abs(random.nextInt()) % connNames.size();
                String name = connNames.get(idx);
                if ((!healthMap.containsKey(name) || healthMap.get(name).isActive()) && send(connMap.get(name))) {
                    i++;
                }
            }
        }
    }

    private void sendRemoval(Connection conn) {
        if (gossipShareCount > connMap.size()) {
            connMap.forEach((k, v) -> send(v, conn));
        } else {
            for (int i = 0; i < gossipShareCount; ) {
                int idx = Math.abs(random.nextInt()) % connNames.size();
                String name = connNames.get(idx);
                if (healthMap.get(name).isActive() && send(connMap.get(name), conn)) {
                    i++;
                }
            }
        }
    }

    private boolean send(Connection to, Connection removed) {
        boolean res = false;
        if (to != null && removed != null) {
            try {
                DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(to.getSocket().getOutputStream()));
                dos.writeUTF("REMOVE: " + removed.name());
                dos.flush();
                res = true;
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }

        return res;
    }

    private boolean send(Connection connection) {
        boolean res = false;
        if (connection != null) {
            try {
                List<HealthDetail> details = new ArrayList<>(healthMap.values());
                selfHealth.incrCounter();
                details.add(selfHealth);
                DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(connection.getSocket().getOutputStream()));
                dos.writeUTF("HEALTH: " + toJson(details));
                dos.flush();
                res = true;
            } catch (IOException ex) {
                System.err.println("Error sending to " + connection.name());
            }
        }

        return res;
    }

    private String toJson(List<HealthDetail> details) {
        JsonArray arr = new JsonArray(details.size());
        for(HealthDetail d : details) {
            JsonObject obj = new JsonObject();
            obj.addProperty("ip", d.getIp());
            obj.addProperty("port", d.getPort());
            obj.addProperty("cntr", d.getCounter());
            obj.addProperty("startTime", d.getStartTime());
            obj.addProperty("lastUpdateTime", d.getLastUpdateTime());
            arr.add(obj);
        }
        return arr.toString();
    }

    private String toConnsJson(List<Connection> conns) {
        JsonArray arr = new JsonArray(conns.size());
        for(Connection c : conns) {
            JsonObject obj = new JsonObject();
            obj.addProperty("ip", c.getIp());
            obj.addProperty("port", c.getPort());
            arr.add(obj);
        }
        return arr.toString();
    }
}
