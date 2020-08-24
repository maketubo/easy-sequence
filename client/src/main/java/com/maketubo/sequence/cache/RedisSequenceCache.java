package com.maketubo.sequence.cache;

import com.maketubo.sequence.RedisSequence;
import com.maketubo.sequence.SequenceManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.RedisTemplate;

import javax.sql.DataSource;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 如果redis没有设置每一操作都同步复制到slave节点（事实上为了效率不会这么设置）,则需要为可能发生的redis宕机启动slave节点
 * 出现部分key改变丢失的情况,需要做补偿控制（比如设置唯一索引等）
 * @author maketubo
 * @version 1.0
 * @ClassName RedisSequenceCache
 * @description
 * @date 2020/8/17 22:24
 * @since JDK 1.8
 */
public class RedisSequenceCache extends SequenceCache {

    private Logger logger = LoggerFactory.getLogger(RedisSequenceCache.class);

    protected RedisTemplate redisTemplate;
    protected Map<Object, RedisSequence> redisSequenceMap = new HashMap<>();

    protected String seqDirKey = "easySeq:cache:";

    public RedisSequenceCache(Class<?> seqClazz, Boolean enableCategory, Boolean enableExternal,
                              DataSource dataSource, RedisTemplate redisTemplate) {
        super(seqClazz, enableCategory, enableExternal, dataSource);
        this.redisTemplate = redisTemplate;
    }

    public void installExternal(){
        try {
            List docSequences = SequenceManager.getExternalSequences(dataSource, seqClazz);
            if (enableCategory) {
                //根据租户id分类
                Map<Object, List<?>> sequencesByCategory = SequenceManager.getSequencesByCategory(docSequences);
                for (Object category : sequencesByCategory.keySet()) {
                    String categoryStr = category == null ? "" : ":".concat(category.toString());
                    redisSequenceMap.put(category,
                            new RedisSequence(redisTemplate, seqDirKey.concat(categoryStr), sequencesByCategory.get(category)));
                }
            } else {
                List collect = SequenceManager.getSequencesWithoutCategory(docSequences);
                redisSequenceMap.put(null, new RedisSequence(redisTemplate, seqDirKey, collect));
            }
        } catch (Exception e) {
            logger.warn("sequence cache may not install correctly");
        }
    }

    @Override
    Long doGetNext(String seqName, int num, String category, boolean createWhenEmpty) {
        String categoryStr = category == null ? "" : ":".concat(category);
        redisSequenceMap.putIfAbsent(category, new RedisSequence(redisTemplate, seqDirKey.concat(categoryStr)));
        RedisSequence redisSequence = redisSequenceMap.get(category);
        return redisSequence.getNext(seqName, num, createWhenEmpty);
    }



}
