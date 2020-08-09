package poc.gossip.model;

public class HealthDetail {
    private final String ip;
    private final int port;
    private final long startTime;

    private long cntr; //Version
    private long lastUpdateTime;

    private boolean isActive;

    public HealthDetail(String ip, int port, long startTime) {
        this.ip = ip;
        this.port = port;
        this.startTime = startTime;
        this.cntr = 0;
        this.lastUpdateTime = System.currentTimeMillis();
        this.isActive = true;
    }

    public String getIp() {
        return ip;
    }

    public int getPort() {
        return port;
    }

    public long getStartTime() {
        return startTime;
    }

    public long getCounter() {
        return cntr;
    }

    public void incrCounter() {
        this.cntr++;
    }

    public long getLastUpdateTime() {
        return lastUpdateTime;
    }

    public boolean isActive() {
        return isActive;
    }

    public void setActive(boolean active) {
        if (isActive != active) {
            System.out.println("Status change for " + name() + ": " + isActive + " -> " + active);
        }
        isActive = active;
    }

    public String name() {
        return String.format("%s:%s", this.ip, this.port);
    }

    public void update(HealthDetail healthDetail) {
        if (null != healthDetail && healthDetail.cntr > cntr) {
            setLastUpdateTime(System.currentTimeMillis());
            setCounter(healthDetail.getCounter());
            setActive(healthDetail.isActive);
        }
    }

    public void setLastUpdateTime(long lastUpdateTime) {
        this.lastUpdateTime = lastUpdateTime;
    }

    public void setCounter(long cntr) {
        this.cntr = cntr;
    }

    @Override
    public boolean equals(Object obj) {
        boolean res = false;
        if (obj instanceof HealthDetail) {
            HealthDetail that = (HealthDetail) obj;
            res = this.ip.equals(that.ip) && this.port == that.port;
        }
        return res;
    }

    @Override
    public String toString() {
        return "[" + ip + ":" + port + " " + cntr + " - " + isActive + "]";
    }
}
