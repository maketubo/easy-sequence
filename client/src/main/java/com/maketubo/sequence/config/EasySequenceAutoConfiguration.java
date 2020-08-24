package com.maketubo.sequence.config;


import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

/**
 * @author maketubo
 * @version 1.0
 * @ClassName EasySequenceAutoConfiguration
 * @description
 * @date 2020/8/16 0:34
 * @since JDK 1.8
 */
@Configuration
@ConditionalOnProperty(prefix = "easy-sequence", name = "enabled", havingValue = "true")
@ConditionalOnWebApplication(type = ConditionalOnWebApplication.Type.SERVLET)
@EnableConfigurationProperties(EasySequenceProperties.class)
@Import({LocalSequenceCacheConfiguration.class,
        RedisSequenceCacheConfiguration.class})
public class EasySequenceAutoConfiguration {

}
