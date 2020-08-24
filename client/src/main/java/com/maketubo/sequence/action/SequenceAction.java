package com.maketubo.sequence.action;

import com.maketubo.sequence.cache.SequenceCache;

/**
 * @author maketubo
 * @version 1.0
 * @ClassName SequenceAction
 * @description
 * @date 2020/8/20 23:17
 * @since JDK 1.8
 */
public class SequenceAction {

    private static SequenceCache core;

    private SequenceAction(){}

    public static void setCore(SequenceCache core) {
        SequenceAction.core = core;
    }

    public static Long getNext(String sequenceName){
        return getNext(sequenceName, null);
    }

    public static Long getNext(String sequenceName, boolean createWhenEmpty){
        return getNext(sequenceName, null, createWhenEmpty);
    }

    public static Long getNext(String sequenceName, String category){
        return getNext(sequenceName, category, false);
    }

    public static Long getNext(String sequenceName, String category, boolean createWhenEmpty){
        return core.getNext(sequenceName, 1, category, createWhenEmpty);
    }

    public static Long getNext(String sequenceName, int num){
        return getNext(sequenceName, num, null);
    }

    public static Long getNext(String sequenceName, int num, String category){
        return getNext(sequenceName, num, category, false);
    }

    public static Long getNext(String sequenceName, int num, String category, boolean createWhenEmpty){
        return core.getNext(sequenceName, num, category, createWhenEmpty);
    }

    public static void action(String sequenceName, CurrentValCallback actionCallback){
        actionCallback.callback(core.getNext(sequenceName));
    }

    public static void action(String sequenceName, int num, RangeValCallback actionCallback){
        Long next = core.getNext(sequenceName);
        actionCallback.callback(next - num, next);
    }

    interface CurrentValCallback {
       void callback(Long currentVal);
    }

    interface RangeValCallback {
        void callback(Long valLeft, Long valRight);
    }


}
