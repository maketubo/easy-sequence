package com.maketubo.sequence;

import java.util.List;

/**
 * @author maketubo
 * @version 1.0
 * @ClassName AbstractSequence
 * @description
 * @date 2020/6/1 14:55
 * @since JDK 1.8
 */
public abstract class AbstractSequence {

    abstract void loadSequences(List<?> externalList);

    public Long getNext(String seqName) {
        return getNext(seqName, false);
    }

    public Long getNext(String seqName, boolean createWhenEmpty) {
        return getNext(seqName, 1, createWhenEmpty);
    }

    /*
     * getNext :
     *
     * @author maketubo
     * @version 1.0
     * @date 2020/8/23 23:09
     * @param seqName
     * @param num
     * @param createWhenEmpty is true return 0 when empty else return null
     * @return java.lang.Long
     * @since JDK 1.8
     */
    abstract Long getNext(String seqName, int num, boolean createWhenEmpty);

}
