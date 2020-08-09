package poc.gossip;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import poc.gossip.model.Connection;
import poc.gossip.model.HealthDetail;
import poc.gossip.service.GossipService;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class Main {

    private final GossipService gossip;
    private final String ip;
    private final int port;

    private Main(String ip, int port) {
        this.ip = ip;
        this.port = port;
        gossip = new GossipService(ip, port);
    }

    public static void main(String[] args) {
        if (args.length == 0) {
            args = new String[]{"1000", "1001"};
        }
        int port = Integer.parseInt(args[0]);
        Main main = new Main("127.0.0.1", port);
        main.startServer(port, args.length == 2 ? Integer.parseInt(args[1]) : -1);
    }

    private void startServer(int port, int peerPort) {
        try {
            ServerSocket server = new ServerSocket(port);
            System.out.println("Started! Waiting for requests...");
            if (peerPort > 0) {
                new Thread(() -> connectTo(peerPort)).start();
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            while (true) {
                Socket client = server.accept();
                System.out.println("New Connection: " + client.getPort());
                new ClientWorker(client).start();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("Server Stopped!");
    }

    private void connectTo(int port) {
        try {
            Socket client = new Socket("127.0.0.1", port);
            System.out.println("Port: " + client.getPort());
            DataOutputStream output = new DataOutputStream(client.getOutputStream());
            DataInputStream input = new DataInputStream(client.getInputStream());
            gossip.register(new Connection(client.getInetAddress().getHostAddress(), port, client), false);
            output.writeUTF("REGISTER: " + this.ip + ":" + this.port);

            String line = input.readUTF();
            while (!line.equals("QUIT")) {
                operate(client, line);
                line = input.readUTF();
            }
            gossip.remove(client);
            output.close();
            input.close();
            System.out.println(client + " quited!");
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    class ClientWorker extends Thread {
        private Socket client;

        ClientWorker(Socket socket) {
            super("Peer-" + socket.getPort());
            this.client = socket;
        }

        @Override
        public void run() {
            try {
                String clientName = client.toString();
                DataInputStream dis = new DataInputStream(new BufferedInputStream(client.getInputStream()));
                DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(client.getOutputStream()));
                String line = dis.readUTF();
                while (!line.equals("QUIT")) {
                    operate(client, line);
                    line = dis.readUTF();
                }
                gossip.remove(client);
                dis.close();
                dos.close();
                System.out.println(clientName + " quited!");
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
    }

    private void operate(Socket client, String line) {
        if (line.startsWith("REMOVE: ")) {
            gossip.updateRemove(line.replace("REMOVE: ", ""));
        } else if (line.startsWith("HEALTH: ")) {
            List<HealthDetail> dtls = parseJsonMessage(line.replace("HEALTH: ", ""));
            gossip.updateHealth(dtls);
        } else if (line.startsWith("REGISTER: ")) {
            String pear = line.replace("REGISTER: ", "");
            System.out.println("New Peer: " + pear);
            String[] arr = pear.split(":");
            gossip.register(new Connection(arr[0], Integer.parseInt(arr[1]), client), true);
        } else if (line.startsWith("CONNS: ")) {
            List<Connection> connections = parseConnections(line.replace("CONNS: ", ""));
            gossip.updateConnection(connections);
        } else {
            System.out.println("Message from " + client + ": " + line);
        }
    }

    private List<HealthDetail> parseJsonMessage(String json) {
        JsonElement element = JsonParser.parseString(json);
        JsonArray jArr = element.getAsJsonArray();
        List<HealthDetail> res = new ArrayList<>(jArr.size());
        Iterator<JsonElement> iter = jArr.iterator();
        while (iter.hasNext()) {
            JsonElement je = iter.next();
            JsonObject jo = je.getAsJsonObject();
            HealthDetail hd = new HealthDetail(
                    jo.get("ip").getAsString(),
                    jo.get("port").getAsInt(),
                    jo.get("startTime").getAsLong());
            hd.setCounter(jo.get("cntr").getAsInt());
            hd.setLastUpdateTime(jo.get("lastUpdateTime").getAsLong());
            res.add(hd);
        }

        return res;
    }

    private List<Connection> parseConnections(String json) {
        JsonElement element = JsonParser.parseString(json);
        JsonArray jArr = element.getAsJsonArray();
        List<Connection> res = new ArrayList<>(jArr.size());
        Iterator<JsonElement> iter = jArr.iterator();
        while (iter.hasNext()) {
            JsonElement je = iter.next();
            JsonObject jo = je.getAsJsonObject();
            String ip = jo.get("ip").getAsString();
            int port = jo.get("port").getAsInt();
            Socket sock = null;
            try {
                System.out.println("Connection: " + ip + " - " + port);
                sock = new Socket(ip, port);
            } catch (IOException e) {
                e.printStackTrace();
            }
            res.add(new Connection(ip, port, sock));
        }

        return res;
    }
}
