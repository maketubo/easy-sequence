package com.maketubo.sequence.cache;

import com.maketubo.sequence.RedisSequence;
import com.maketubo.sequence.SequenceManager;

import javax.sql.DataSource;
import java.util.HashMap;
import java.util.Map;

/**
 * @author maketubo
 * @version 1.0
 * @ClassName SequenceCache
 * @description
 * @date 2020/1/16 23:56
 * @since JDK 1.8
 */
public abstract class SequenceCache {

    protected Boolean enableCategory;
    protected Boolean enableExternal;
    protected DataSource dataSource;

    Class<?> seqClazz;

    public SequenceCache(Class<?> seqClazz, Boolean enableCategory) {
        this.seqClazz = seqClazz;
        this.enableCategory = enableCategory;
    }

    public SequenceCache(Class<?> seqClazz, Boolean enableCategory, Boolean enableExternal, DataSource dataSource) {
        this.seqClazz = seqClazz;
        this.enableCategory = enableCategory;
        this.enableExternal = enableExternal;
        this.dataSource = dataSource;
        if(enableExternal) {
            installExternal();
        }
    }

    abstract void installExternal();
    public Long getNext(String seqName, int num, String category, boolean createWhenEmpty){
        Long result = doGetNext(seqName, num, category, createWhenEmpty);
        if(enableExternal) {
            SequenceManager.addSequenceAsyncUtilFull(dataSource, seqClazz, seqName, result, category);
        }
        return result;
    }
    abstract Long doGetNext(String seqName, int num, String category, boolean createWhenEmpty);

    public Long getNext(String seqName) {
        return getNext(seqName, false);
    }

    public Long getNext(String seqName, boolean createWhenEmpty) {
        return getNext(seqName, 1, null, createWhenEmpty);
    }

    public Long getNext(String seqName, String category) {
        return getNext(seqName, category, false);
    }

    public Long getNext(String seqName, String category, boolean createWhenEmpty) {
        return getNext(seqName, 1, category, createWhenEmpty);
    }




}
