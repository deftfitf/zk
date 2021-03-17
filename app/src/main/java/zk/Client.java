package zk;

import org.apache.zookeeper.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

public class Client implements Watcher {
    private static final Logger logger = LoggerFactory.getLogger(Client.class);
    ZooKeeper zk;
    String hostPort;

    Client(String hostPort) {
        this.hostPort = hostPort;
    }

    void startZk() throws IOException {
        zk = new ZooKeeper(hostPort, 15000, this);
    }

    void stopZk() throws InterruptedException {
        zk.close();
    }

    CompletableFuture<String> queueCommand(String command) {
        final var future = new CompletableFuture<String>();
        queueCommand(command, future);
        return future;
    }

    private void queueCommand(String command, CompletableFuture<String> future) {
        zk.create(
                "/tasks/task-",
                command.getBytes(StandardCharsets.UTF_8),
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT_SEQUENTIAL,
                ((rc, path, ctx, name) -> {
                    switch (KeeperException.Code.get(rc)) {
                        case OK:
                            future.complete(name);
                            break;

                        case NODEEXISTS:
                            future.completeExceptionally(new RuntimeException(name + " already appears to be running"));
                            break;

                        case CONNECTIONLOSS:
                            if (future.isCancelled()) {
                                logger.info(command + " queueing canceled");
                                break;
                            }
                            queueCommand(command, future);
                            break;

                        default:
                            future.completeExceptionally(new RuntimeException(name + " queueing failed"));
                    }
                }),
                command
        );
    }

    @Override
    public void process(WatchedEvent event) {
        logger.info("{}", event);
    }

    public static void main(String[] args) throws Exception {
        final var client = new Client("127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183");

        client.startZk();

        client.queueCommand(UUID.randomUUID().toString()).join();

        client.stopZk();
    }

}
