package com.maketubo.sequence.config;

import com.maketubo.sequence.cache.LocalSequenceCache;
import com.maketubo.sequence.cache.RedisSequenceCache;
import com.maketubo.sequence.cache.SequenceCache;
import org.apache.curator.framework.CuratorFramework;
import org.springframework.data.redis.core.RedisTemplate;

import javax.sql.DataSource;

/**
 * @author maketubo
 * @version 1.0
 * @ClassName SequenceBuilder
 * @description
 * @date 2020/8/17 22:37
 * @since JDK 1.8
 */
public class SequenceCacheBuilder {

    private DataSource dataSource;

    private Class<?> seqClazz;

    private SequenceMode mode;

    private RedisTemplate redisTemplate;

    private CuratorFramework zkClient;

    private Boolean enableCategory = false;
    private Boolean enableExternal = false;


    public static SequenceCacheBuilder newSequenceCache() {
        return new SequenceCacheBuilder();
    }

    public SequenceCacheBuilder withDataSource(DataSource datasource) {
        this.dataSource = datasource;
        return this;
    }

    public <T> SequenceCacheBuilder forSequence( Class<?> clazz) {
        this.seqClazz = clazz;
        return this;
    }

    public <T> SequenceCacheBuilder forSequence(String className) {
        Class<?> clazz;
        try {
            clazz = Class.forName(className);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("cannot find sequence entity class", e);
        }
        this.seqClazz = clazz;
        return this;
    }

    public SequenceCacheBuilder onMode(SequenceMode mode) {
        this.mode = mode;
        return this;
    }

    public SequenceCacheBuilder enableCategory(boolean enabled) {
        this.enableCategory = enabled;
        return this;
    }

    public SequenceCacheBuilder enableExternal(boolean external) {
        this.enableExternal = external;
        return this;
    }

    public SequenceCacheBuilder withRedisTemplate(RedisTemplate redisTemplate) {
        this.redisTemplate = redisTemplate;
        return this;
    }

    public SequenceCacheBuilder withCuratorFramework(CuratorFramework zkClient) {
        this.zkClient = zkClient;
        return this;
    }

    public SequenceCache build() {
        if((dataSource == null || redisTemplate == null) && !mode.equals(SequenceMode.single)) {
            throw new RuntimeException("please setup datasource and redisTemplate when mode is not single node");
        }
        switch (mode) {
            case single:
                if(enableExternal) {
                    return new LocalSequenceCache(seqClazz, enableCategory, enableExternal, dataSource);
                } else {
                    return new LocalSequenceCache(seqClazz, enableCategory);
                }
            case redis:
                return new RedisSequenceCache(seqClazz, enableCategory, enableExternal, dataSource, redisTemplate);
            default:
                throw new RuntimeException("not support mode");
        }
    }

}
