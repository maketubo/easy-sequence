package com.maketubo.sequence.config;

import com.maketubo.sequence.action.SequenceAction;
import com.maketubo.sequence.cache.SequenceCache;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.sql.DataSource;

/**
 * @author maketubo
 * @version 1.0
 * @ClassName LocalSequenceCacheConfiguration
 * @description
 * @date 2020/8/22 23:18
 * @since JDK 1.8
 */
@ConditionalOnProperty(prefix = "easy-sequence", name = "mode", havingValue = "single")
public class LocalSequenceCacheConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public SequenceCache localSequenceCache(EasySequenceProperties properties, ObjectProvider<DataSource> provider){
        SequenceMode mode = properties.getMode();
        SequenceCacheBuilder sequenceCacheBuilder = SequenceCacheBuilder.newSequenceCache()
                .withDataSource(provider.getIfAvailable())
                .forSequence(properties.getProvider())
                .enableCategory(properties.isCategory())
                .enableExternal(properties.isExternal())
                .onMode(mode);
        SequenceCache cache = sequenceCacheBuilder.build();
        SequenceAction.setCore(cache);
        return cache;
    }
}
