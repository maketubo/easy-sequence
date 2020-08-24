package com.maketubo.sequence.util;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.zookeeper.data.Stat;

import java.util.function.Consumer;

/**
 * @author maketubo
 * @version 1.0
 * @ClassName ZkCacheWather
 * @description
 * @date 2020/6/12 14:38
 * @since JDK 1.8
 */
public class ZkCacheWatcher {

    public static void change(CuratorFramework client, String key, String change) {
        try {
            client.setData().forPath(key, change.getBytes());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static void watch(CuratorFramework client, String key, Consumer<byte[]> consumer) {
        try {
            Stat stat = client.checkExists().forPath(key);
            if(stat == null){
                client.create().creatingParentsIfNeeded().forPath(key);
            }
            NodeCache nodeCache = new NodeCache(client, key);
            //调用start方法开始监听
            nodeCache.start();
            //添加NodeCacheListener监听器
            nodeCache.getListenable().addListener(() ->
                    consumer.accept(nodeCache.getCurrentData().getData()));
        } catch (Exception e){
            throw new RuntimeException(e);
        }
    }
}
