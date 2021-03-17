package zk;

import org.apache.zookeeper.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Random;
import java.util.concurrent.CompletableFuture;

public class Worker implements Watcher {
    private static final Logger logger = LoggerFactory.getLogger(Worker.class);
    private static final String serverId = Integer.toHexString(new Random().nextInt());
    ZooKeeper zk;
    String hostPort;
    String status;

    public Worker(String hostPort) {
        this.hostPort = hostPort;
    }

    void startZk() throws IOException {
        zk = new ZooKeeper(hostPort, 15000, this);
    }

    void stopZk() throws InterruptedException {
        zk.close();
    }

    CompletableFuture<Void> registerAsync() {
        final var future = new CompletableFuture<Void>();
        registerAsync(future);
        return future;
    }

    void registerAsync(CompletableFuture<Void> future) {
        zk.create(
                "/workers/worker-" + serverId,
                "Idle".getBytes(StandardCharsets.UTF_8),
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL,
                ((rc, path, ctx, name) -> {
                    switch (KeeperException.Code.get(rc)) {
                        case CONNECTIONLOSS:
                            registerAsync(future);
                            break;
                        case OK:
                            logger.info("Registered successfully: " + serverId);
                            future.complete(null);
                            break;
                        case NODEEXISTS:
                            logger.warn("Already Registered: " + serverId);
                            future.complete(null);
                            break;
                        default:
                            future.completeExceptionally(
                                    KeeperException.create(KeeperException.Code.get(rc)));
                    }
                }),
                null
        );
    }

    CompletableFuture<Void> setStatus(String status) {
        this.status = status;
        final var future = new CompletableFuture<Void>();
        updateStatusAsync(future, status);
        return future;
    }

    synchronized private void updateStatusAsync(CompletableFuture<Void> future, String status) {
        if (status.equals(this.status)) {
            zk.setData(
                    "/workers/worker-" + serverId,
                    status.getBytes(StandardCharsets.UTF_8),
                    -1,
                    ((rc, path, ctx, stat) -> {
                        switch (KeeperException.Code.get(rc)) {
                            case OK:
                                future.complete(null);
                                return;
                            case CONNECTIONLOSS:
                                updateStatusAsync(future, (String) ctx);
                                return;
                            default:
                                future.completeExceptionally(
                                        KeeperException.create(KeeperException.Code.get(rc)));

                        }
                    }),
                    status);
        }
    }

    @Override
    public void process(WatchedEvent event) {
        logger.info("{}, {}", event, hostPort);
    }

    public static void main(String[] args) throws Exception {
        final var worker = new Worker("127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183");

        worker.startZk();

        worker.registerAsync().join();

        worker.setStatus("1");
        worker.setStatus("3");
        worker.setStatus("8");

        Thread.sleep(60000);

        worker.stopZk();
    }


}