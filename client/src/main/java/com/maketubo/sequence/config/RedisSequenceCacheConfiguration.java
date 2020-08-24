package com.maketubo.sequence.config;

import com.maketubo.sequence.action.SequenceAction;
import com.maketubo.sequence.cache.SequenceCache;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.data.redis.core.StringRedisTemplate;

import javax.sql.DataSource;

/**
 * @author maketubo
 * @version 1.0
 * @ClassName RedisSequenceCacheConfiguration
 * @description
 * @date 2020/8/22 23:19
 * @since JDK 1.8
 */
@ConditionalOnBean({ DataSource.class, StringRedisTemplate.class })
@ConditionalOnProperty(prefix = "easy-sequence", name = "mode", havingValue = "redis")
public class RedisSequenceCacheConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public SequenceCache redisSequenceCache(EasySequenceProperties properties, ApplicationContext context) {
        SequenceMode mode = properties.getMode();
        SequenceCacheBuilder sequenceCacheBuilder = SequenceCacheBuilder.newSequenceCache()
                .withDataSource(context.getBean(DataSource.class))
                .forSequence(properties.getProvider())
                .enableCategory(properties.isCategory())
                .enableExternal(properties.isExternal())
                .onMode(mode);
        SequenceCache cache = sequenceCacheBuilder.withRedisTemplate(context.getBean(StringRedisTemplate.class)).build();
        SequenceAction.setCore(cache);
        return cache;
    }

}
