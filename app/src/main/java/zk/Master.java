package zk;

import org.apache.zookeeper.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Random;
import java.util.concurrent.CompletableFuture;

public class Master implements Watcher {
    private static final String serverId = Integer.toHexString(new Random().nextInt());
    private static final Logger logger = LoggerFactory.getLogger(Master.class);

    ZooKeeper zk;
    String hostPort;
    boolean isLeader;

    Master(String hostPort) {
        this.hostPort = hostPort;
    }

    void startZk() throws IOException {
        zk = new ZooKeeper(hostPort, 15000, this);
    }

    void stopZk() throws InterruptedException {
        zk.close();
    }

    CompletableFuture<Boolean> runForMasterAsync() {
        final var future = new CompletableFuture<Boolean>();
        runForMasterAsync(future);
        return future;
    }

    private void runForMasterAsync(CompletableFuture<Boolean> future) {
        if (future.isCancelled()) {
            return;
        }

        zk.create(
                "/master",
                serverId.getBytes(StandardCharsets.UTF_8),
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL,
                ((rc, path, ctx, name) -> {
                    switch (KeeperException.Code.get(rc)) {
                        case CONNECTIONLOSS:
                            checkMasterAsync(future);
                            return;
                        case OK:
                            isLeader = true;
                            future.complete(true);
                            break;
                        default:
                            isLeader = false;
                            future.complete(false);
                    }
                    logger.info("I'm " + (isLeader ? "" : "not ") + "the leader");
                }),
                null
        );
    }

    private void checkMasterAsync(CompletableFuture<Boolean> future) {
        if (future.isCancelled()) {
            return;
        }

        zk.getData(
                "/master",
                false,
                ((rc, path, ctx, data, stat) -> {
                    switch (KeeperException.Code.get(rc)) {
                        case CONNECTIONLOSS:
                            checkMasterAsync(future);
                            return;
                        case NONODE:
                            runForMasterAsync(future);
                            return;
                        default:
                            future.completeExceptionally(
                                    KeeperException.create(KeeperException.Code.get(rc)));
                    }
                }),
                null
        );
    }

    void bootstrap() {
        createParent("/workers", new byte[0]);
        createParent("/assign", new byte[0]);
        createParent("/tasks", new byte[0]);
        createParent("/status", new byte[0]);
    }

    void createParent(String path, byte[] data) {
        zk.create(
                path,
                data,
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT,
                ((rc, path1, ctx, name) -> {
                    switch (KeeperException.Code.get(rc)) {
                        case CONNECTIONLOSS:
                            createParent(path, (byte[]) ctx);

                            break;

                        case OK:
                            logger.info("Parent created");

                            break;
                        case NODEEXISTS:
                            logger.warn("Parent already registered: " + path1);
                            break;

                        default:
                            logger.error("Something went wrong: ",
                                    KeeperException.create(KeeperException.Code.get(rc), path1));
                    }
                }),
                data
        );
    }

    @Override
    public void process(WatchedEvent event) {
        logger.info("{}, {}", event, hostPort);
    }

    public static void main(String[] args) throws Exception {
        final var master = new Master("127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183");

        master.startZk();
        master.bootstrap();

        master.runForMasterAsync().join();

        if (master.isLeader) {
            System.out.println("I'm the leader");
            try {
                Thread.sleep(60000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        } else {
            System.out.println("Someone else is the leader");
        }

        master.stopZk();
    }

}