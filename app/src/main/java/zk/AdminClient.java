package zk;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class AdminClient implements Watcher {
    private static final Logger logger = LoggerFactory.getLogger(AdminClient.class);
    ZooKeeper zk;
    String hostPort;

    AdminClient(String hostPort) {
        this.hostPort = hostPort;
    }

    void startZk() throws Exception {
        zk = new ZooKeeper(hostPort, 15000, this);
    }

    void stopZk() throws InterruptedException {
        zk.close();
    }

    @Override
    public void process(WatchedEvent event) {
        logger.info("{}", event);
    }

    public static class Health {
        final String name;
        final String state;
        final Date startDate;

        public Health(String name, String state, Date startDate) {
            this.name = name;
            this.state = state;
            this.startDate = startDate;
        }

        @Override
        public String toString() {
            return "Health{" +
                    "name='" + name + '\'' +
                    ", state='" + state + '\'' +
                    ", startDate=" + startDate +
                    '}';
        }

        public String getName() {
            return name;
        }

        public String getState() {
            return state;
        }

        public Date getStartDate() {
            return startDate;
        }
    }

    public static class Task {
        final String data;

        Task(String data) {
            this.data = data;
        }

        @Override
        public String toString() {
            return "Task{" +
                    "data='" + data + '\'' +
                    '}';
        }

        public String getData() {
            return data;
        }

    }

    CompletableFuture<List<Health>> listState() {
        CompletableFuture<List<Health>> result =
                CompletableFuture.completedFuture(new ArrayList<>());

        result = result.thenCombineAsync(
                getHealth("/", "master"),
                (lst, health) -> {
                    lst.add(health);
                    return lst;
                });

        final var workers = getChildren("/workers").join();
        for (String worker : workers) {
            result = result.thenCombineAsync(
                    getHealth("/workers/", worker),
                    (lst, health) -> {
                        lst.add(health);
                        return lst;
                    });
        }

        return result;
    }

    CompletableFuture<List<Task>> listTask() {
        return getChildren("/assign").thenApplyAsync(
                tasks -> tasks.stream().map(Task::new).collect(Collectors.toList()));
    }

    private CompletableFuture<Health> getHealth(String pathPrefix, String name) {
        final var health = new CompletableFuture<Health>();
        zk.getData(
                pathPrefix + name,
                false,
                ((rc, path, ctx, data, stat) -> {
                    final var code = KeeperException.Code.get(rc);
                    switch (code) {
                        case OK:
                            health.complete(new Health(
                                    name,
                                    new String(data, StandardCharsets.UTF_8),
                                    new Date(stat.getCtime())));
                        case NONODE:
                            health.complete(new Health(
                                    name,
                                    "No Node",
                                    null));
                            break;
                        default:
                            health.completeExceptionally(KeeperException.create(code));
                    }
                }),
                null
        );
        return health;
    }

    private CompletableFuture<List<String>> getChildren(String node) {
        final var workers = new CompletableFuture<List<String>>();
        zk.getChildren(
                node,
                false,
                ((rc, path, ctx, children) -> {
                    final var code = KeeperException.Code.get(rc);
                    switch (KeeperException.Code.get(rc)) {
                        case OK:
                            workers.complete(children);
                            break;

                        case NONODE:
                            workers.complete(List.of());
                            break;

                        default:
                            workers.completeExceptionally(KeeperException.create(code));
                    }
                }),
                null);
        return workers;
    }

    public static void main(String[] args) throws Exception {
        final var adminClient = new AdminClient("127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183");

        adminClient.startZk();

        final var executor = Executors.newSingleThreadScheduledExecutor();
        executor.scheduleAtFixedRate(() -> {
            logger.info("{}", adminClient.listState().join());
            logger.info("{}", adminClient.listTask().join());
        }, 0, 5, TimeUnit.SECONDS);

        Thread.sleep(60000);
        executor.shutdown();

        adminClient.stopZk();
    }

}
