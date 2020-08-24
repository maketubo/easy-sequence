package com.maketubo.sequence.util;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.x.async.AsyncCuratorFramework;

import java.util.function.Consumer;

/**
 * @author maketubo
 * @version 1.0
 * @ClassName ZkWatcher
 * @description
 * @date 2020/6/7 23:13
 * @since JDK 1.8
 */
public class ZkWatcher {

    public static void change(CuratorFramework client, String key, String change) {
        AsyncCuratorFramework async = AsyncCuratorFramework.wrap(client);
        async.setData().forPath(key, change.getBytes());
    }

    public static void watch(CuratorFramework client, String key, Consumer<byte[]> consumer) {
        try {
            client.create().creatingParentContainersIfNeeded().forPath(key);
            AsyncCuratorFramework async = AsyncCuratorFramework.wrap(client);
            async.watched()
                    .getData()
                    .forPath(key)
                    .event()
                    .thenAccept(watchedEvent -> {
                        try {
                            consumer.accept(client.getData().forPath(key));
                            client.delete().deletingChildrenIfNeeded().forPath(key);
                        } catch (Exception e) {
                            // do nothing ...
                            e.printStackTrace();
                        }
                    });
        } catch (Exception e){
            throw new RuntimeException(e);
        }
    }
}
