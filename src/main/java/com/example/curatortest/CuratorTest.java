package com.example.curatortest;

import com.google.gson.*;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.curator.retry.RetryNTimes;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.List;

import static org.apache.zookeeper.Watcher.Event.EventType.*;

@Component
public class CuratorTest {

    final private String KAFKA_PATH = "/brokers/ids";
    final private String ZK_PATH = "localhost:2181";

    @PostConstruct
    public void getKafkaBootstrapServers() throws Exception {
        int sleepMsBetweenRetries = 100;
        int maxRetries = 3;

        RetryPolicy retryPolicy = new RetryNTimes(maxRetries, sleepMsBetweenRetries);
        CuratorFramework client = CuratorFrameworkFactory.newClient(ZK_PATH, retryPolicy);
        client.start();
        System.out.println("Zookeeper client started!");
        String path = KAFKA_PATH + "/" + client.getChildren().forPath(KAFKA_PATH).get(0);

        updateData(client, path);
        client.getChildren().usingWatcher((CuratorWatcher) event -> {
            if (event.getType().equals(NodeChildrenChanged)) {
                List<String> brokers = client.getChildren().forPath(KAFKA_PATH);
                while (brokers.size() < 1) {
                    System.out.println("WAITING...");
                    Thread.sleep(30000);
                    System.out.println("CHECK!");
                    brokers = client.getChildren().forPath(KAFKA_PATH);
                }
                updateData(client, path);
                getKafkaBootstrapServers();
                System.out.println("CHANGE!");
            }
        }).forPath(KAFKA_PATH);

/*        client.getData().usingWatcher((CuratorWatcher) event -> {
            if (event.getType().equals(NodeDeleted) ||
                    event.getType().equals(NodeDataChanged) ||
                    event.getType().equals(NodeChildrenChanged) ||
                    event.getType().equals(NodeCreated)) {
                updateData(client, path);
                getKafkaBootstrapServers();
                System.out.println("CHANGE!");
            }
        }).forPath(path);*/

    }

    private void updateData(CuratorFramework client, String path) throws Exception {
        client.getChildren().watched().forPath(KAFKA_PATH);
        String zookeeperResponse = new String(client.getData().watched().forPath(path));
        JsonParser jsonParser = new JsonParser();
        JsonObject jo = (JsonObject)jsonParser.parse(zookeeperResponse);
        String host = jo.get("host").getAsString();
        String port = jo.get("port").getAsString();
        System.out.println(host+":"+port);
    }

}
