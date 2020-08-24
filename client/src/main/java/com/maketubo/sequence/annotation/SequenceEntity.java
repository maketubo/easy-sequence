package com.maketubo.sequence.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * @author maketubo
 * @version 1.0
 * @ClassName SequenceEntity
 * @description
 * @date 2020/5/31 23:29
 * @since JDK 1.8
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface SequenceEntity {

    String tableName() default "";

}
