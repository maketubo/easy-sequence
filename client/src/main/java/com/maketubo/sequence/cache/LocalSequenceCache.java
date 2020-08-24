package com.maketubo.sequence.cache;

import com.maketubo.sequence.LocalSequence;
import com.maketubo.sequence.SequenceManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.StampedLock;

/**
 * @author maketubo
 * @version 1.0
 * @ClassName LocalSequenceCache
 * @description
 * @date 2020/8/17 22:25
 * @since JDK 1.8
 */
public class LocalSequenceCache extends SequenceCache {

    private Logger logger = LoggerFactory.getLogger(LocalSequenceCache.class);

    public LocalSequenceCache(Class<?> seqClazz, Boolean enableCategory) {
        super(seqClazz, enableCategory);
    }

    public LocalSequenceCache(Class<?> seqClazz, Boolean enableCategory, Boolean enableExternal, DataSource dataSource) {
        super(seqClazz, enableCategory, enableExternal, dataSource);
    }

    protected Map<Object, LocalSequence> localSequenceMap = new HashMap<>();
    private StampedLock stampedLock = new StampedLock();

    @Override
    public void installExternal() {
        try {
            List docSequences = SequenceManager.getExternalSequences(dataSource, seqClazz);
            if (enableCategory) {
                //根据租户id分类
                Map<Object, List<?>> sequencesByCategory = SequenceManager.getSequencesByCategory(docSequences);
                for (Object category : sequencesByCategory.keySet()) {
                    localSequenceMap.put(category,
                            new LocalSequence(category, sequencesByCategory.get(category)));
                }
            } else {
                List collect = SequenceManager.getSequencesWithoutCategory(docSequences);
                localSequenceMap.put(null, new LocalSequence(null, collect));
            }
        } catch (Exception e) {
            logger.warn("sequence cache may not install correctly");
        }
    }

    @Override
    public Long getNext(String seqName, int num, String category, boolean createWhenEmpty) {
        // 尝试读 状态无变更 直接读
        long stamped = stampedLock.tryOptimisticRead();
        if (!stampedLock.validate(stamped)) {
            //说明值已经被变更了 加锁读
            stamped = stampedLock.readLock();
            try {
                return doGetNext(seqName, num, category, createWhenEmpty);
            } finally {
                stampedLock.unlockRead(stamped);
            }
        }
        //直接读
        return doGetNext(seqName, num, category, createWhenEmpty);
    }

    @Override
    protected Long doGetNext(String seqName, int num, String category, boolean createWhenEmpty) {
        localSequenceMap.putIfAbsent(category, new LocalSequence(category, null));
        LocalSequence localSequence = localSequenceMap.get(category);
        return localSequence.getNext(seqName, num, createWhenEmpty);
    }

}
