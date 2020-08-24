package com.maketubo.sequence.util;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;


/**
 * @author maketubo
 * @version 1.0
 * @ClassName ZkLock
 * @description
 * @date 2020/6/12 15:44
 * @since JDK 1.8
 */
public class ZkLock {

    public static void lock(CuratorFramework client, String key, CallBack callback){
        InterProcessMutex lock = new InterProcessMutex(client, key);
        try {
            lock.acquire();
            callback.call();
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            try {
                lock.release();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

    }

    public interface CallBack{
       void call();
    }
}
