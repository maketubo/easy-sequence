package com.maketubo.sequence;

/**
 * @author maketubo
 * @version 1.0
 * @ClassName SeqSqlMethod
 * @description
 * @date 2020/7/6 23:14
 * @since JDK 1.8
 */
public enum SeqSqlMethod {

    INSERT("insert into %s (%s) VALUES(%s)"),
    DELETE("delete from %s where %s"),
    UPDATE("update %s set %s where %s"),
    QUERY_ALL("select %s, %s, %s from %s");

    String sql;

    SeqSqlMethod(String sql) {
        this.sql = sql;
    }

    public String getSql() {
        return sql;
    }

}
