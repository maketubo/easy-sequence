package com.maketubo.sequence;

import com.maketubo.sequence.annotation.SeqCategory;
import com.maketubo.sequence.annotation.SeqNameField;
import com.maketubo.sequence.annotation.SeqNextValField;
import com.maketubo.sequence.annotation.SequenceEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

import javax.sql.DataSource;
import java.lang.reflect.Field;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 *
 * @author maketubo
 * @version 1.0
 * @ClassName SequenceHolder
 * @description
 * @date 2020/1/17 0:17
 * @since JDK 1.8
 */
public class SequenceManager {

    private static Logger logger = LoggerFactory.getLogger(SequenceManager.class);

    private static final int BATCH_SIZE = 100;

    private static final int BATCH_TIME = 1000;

    private static Map<Class, SequenceInfo> map = new HashMap<>();

    private static volatile int count = 1;

    private static Long lastTime = 0l;

    private static ExecutorService dbExecutorService = new ThreadPoolExecutor(0, 5,
                                      60L, TimeUnit.SECONDS,
                                      new SynchronousQueue<Runnable>());

    private static List<NameAndValue> sequences = new ArrayList<>();

    private static ReentrantLock lock = new ReentrantLock();

    private SequenceManager(){}

    public static <T> List<T> getExternalSequences(DataSource dataSource, Class<T> clazz){
        SequenceInfo sequenceInfo = loadFields(clazz);
        List<T> result = new ArrayList<>();
        try {
            Statement statement = dataSource.getConnection().createStatement();
            statement.setFetchSize(1000);
            ResultSet resultSet = statement.executeQuery(sequenceInfo.selectSql);
            String seqNextValKey = camel2Underscore(sequenceInfo.seqNextValFieldName);
            String seqNameKey = camel2Underscore(sequenceInfo.seqNameFieldName);
            String seqCategoryKey = null;
            if(sequenceInfo.seqCategoryField != null) {
                seqCategoryKey = camel2Underscore(sequenceInfo.seqCategoryFieldName);
            }
            while (resultSet.next()) {
                String seqName = resultSet.getString(seqNameKey);
                Long seqNextVal = resultSet.getLong(seqNextValKey);
                Object seqCategory = sequenceInfo.seqCategoryField != null ? resultSet.getObject(seqCategoryKey,
                        sequenceInfo.seqCategoryField.getType()) : null;
                T target = buildSequence(seqName, seqNextVal, seqCategory, clazz);
                result.add(target);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return result;
    }

    public static SequenceInfo loadFields(Class<?> clazz){
        SequenceInfo sequenceInfo = map.get(clazz);
        if (sequenceInfo != null){
            return sequenceInfo;
        }
        SequenceEntity sequenceEntity = clazz.getAnnotation(SequenceEntity.class);
        String tableName = sequenceEntity.tableName();
        if(tableName == null || tableName.trim().length() == 0) {
            tableName = camel2Underscore(clazz.getSimpleName());
        }
        Field seqNameField = null; String seqNameFieldName = null;
        Field seqNextValField = null; String seqNextValFieldName = null;
        Field seqCategoryField = null; String seqCategoryFieldName = null;
        // 会读取包括父类
        Class<?> targetClass = clazz;
        do {
            Field[] fields = clazz.getDeclaredFields();
            for (Field field : fields) {
                try {
                    if (field.getAnnotation(SeqNameField.class) != null) {
                        seqNameField = field;
                        if(!seqNameField.isAccessible()){
                            seqNameField.setAccessible(true);
                        }
                        seqNameFieldName = field.getAnnotation(SeqNameField.class).alias();
                        if(seqNameFieldName == null || seqNameFieldName.trim().length() == 0) {
                            seqNameFieldName = camel2Underscore(field.getName());
                        }
                    }
                    if (field.getAnnotation(SeqNextValField.class) != null) {
                        seqNextValField = field;
                        if(!seqNextValField.isAccessible()){
                            seqNextValField.setAccessible(true);
                        }
                        seqNextValFieldName = field.getAnnotation(SeqNextValField.class).alias();
                        if(seqNextValFieldName == null || seqNextValFieldName.trim().length() == 0) {
                            seqNextValFieldName = camel2Underscore(field.getName());
                        }
                    }
                    if (field.getAnnotation(SeqCategory.class) != null) {
                        seqCategoryField = field;
                        if(!seqCategoryField.isAccessible()){
                            seqCategoryField.setAccessible(true);
                        }
                        seqCategoryFieldName = field.getAnnotation(SeqCategory.class).alias();
                        if(seqCategoryFieldName == null || seqCategoryFieldName.trim().length() == 0) {
                            seqCategoryFieldName = camel2Underscore(field.getName());
                        }
                    }
                } catch (Exception ex) {
                    //do nothing
                    logger.warn("解析SequenceEntity遇到错误");
                    ex.printStackTrace();
                }
            }
            targetClass = targetClass.getSuperclass();
        } while (targetClass != null && targetClass != Object.class);

        String selectSql = String.format(SeqSqlMethod.QUERY_ALL.getSql(), seqNameFieldName, seqNextValFieldName, seqCategoryFieldName,
                tableName);
        String updateSql = String.format(SeqSqlMethod.UPDATE.getSql(), tableName,
                Arrays.asList(seqCategoryFieldName, seqNameFieldName, seqNextValFieldName)
                        .stream().filter(Objects::nonNull).collect(Collectors.joining("=?,","","=?")),
                Arrays.asList(seqCategoryFieldName, seqNameFieldName)
                        .stream().filter(Objects::nonNull).collect(Collectors.joining("=? and ",
                        "","=? and " + seqNameFieldName + " < ?"))
                );
        sequenceInfo = new SequenceInfo(seqNameField, seqNameFieldName, seqNextValField, seqNextValFieldName,
                seqCategoryField, seqCategoryFieldName, tableName, selectSql, updateSql);
        map.put(clazz, sequenceInfo);
        return sequenceInfo;
    }


    public static  <T> Map<String, Long> getSequenceMap(List<T> sequences) {
        Assert.isTrue(sequences != null || !sequences.isEmpty(), "sequence must not empty");
        Map<String, Long> sequenceMap = new HashMap<>();
        Class<?> clazz = sequences.get(0).getClass();
        SequenceInfo sequenceInfo = loadFields(clazz);
        try {
            for (T obj : sequences){
                String seqName = (String) sequenceInfo.seqNameField.get(obj);
//                Assert.isTrue(seqName.getBytes(Charset.forName("UTF-8")).length <= 32,
//                        "序列名请不要超过32位");
                if(seqName.getBytes(Charset.forName("UTF-8")).length > 32){
                    continue;
                }
                sequenceMap.putIfAbsent(seqName, (Long) sequenceInfo.seqNextValField.get(obj));
            }
        } catch (Exception e){
            throw new RuntimeException("反射获取属性时发生异常", e);
        }
        return sequenceMap;
    }

    public static void addSequenceAsyncUtilFull(DataSource dataSource, Class<?> clazz, String seqName, Long num, String category) {
        try {
            lock.lock();
            SequenceInfo sequenceInfo = loadFields(clazz);
            sequences.add(new NameAndValue(seqName, num, category));
            // 达到阈值触发保存
            if (count == BATCH_SIZE || System.currentTimeMillis() - lastTime > BATCH_TIME) {
                List<NameAndValue> cloned = new ArrayList<>();
                cloned.addAll(sequences);
                sequences.clear();
                refreshDbSequence(dataSource, cloned, sequenceInfo);
                count = 1;
                lastTime = System.currentTimeMillis();
            } else {
                Assert.isTrue(count < BATCH_SIZE, "count >= BatchSize is not expected");
                count++;
            }
        } finally {
            lock.unlock();
        }
    }

    private static void refreshDbSequence(DataSource dataSource, List<NameAndValue> cloned, SequenceInfo sequenceInfo) {
        dbExecutorService.execute(() -> {
            try {
                Connection connection = dataSource.getConnection();
                PreparedStatement preparedStatement = connection.prepareStatement(sequenceInfo.updateSql);
                cloned.forEach(seq -> {
                    try {
                        if(sequenceInfo.seqCategoryField == null){
                            preparedStatement.setObject(1, seq.seqVal);
                            preparedStatement.setObject(2, seq.seqName);
                            preparedStatement.setObject(3, seq.seqVal);
                        } else {
                            preparedStatement.setObject(1, seq.seqVal);
                            preparedStatement.setObject(2, seq.category);
                            preparedStatement.setObject(3, seq.seqName);
                            preparedStatement.setObject(4, seq.seqVal);
                        }
                        int i = preparedStatement.executeUpdate();
                        if (i == 0) {
                            logger.warn("lower value try to write to db or docType not exists");
                        }
                    } catch (Exception e) {
                        logger.error("incrSequenceNumber error please check", e);
                    }
                });
            } catch (Exception e) {
                logger.error("incrSequenceNumber error please check", e);
            }
        });
    }

    public static String camel2Underscore(String text){
        Matcher m = Pattern.compile("(?<=[a-z])[A-Z]").matcher(text);

        StringBuffer sb = new StringBuffer();
        while (m.find()) {
            m.appendReplacement(sb, "_" + m.group().toLowerCase());
        }
        m.appendTail(sb);
        return sb.toString();
    }

    public static <T> Object getCategory(T sequence){
        SequenceInfo sequenceInfo = loadFields(sequence.getClass());
        try {
            return sequenceInfo.seqCategoryField.get(sequenceInfo);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    public static <T> Map<Object, List<T>> getSequencesByCategory(List<T> docSequences) {
        return docSequences.stream()
                .filter(doc -> getCategory(doc) != null)
                .collect(Collectors.groupingBy(doc -> getCategory(doc)));
    }

    public static <T> List<T> getSequencesWithoutCategory(List<T> docSequences) {
        return docSequences.stream()
                .filter(doc -> getCategory(doc) == null)
                .collect(Collectors.toList());
    }

    public static <T> T buildSequence(String seqName, Long seqNextVal, Object seqCategory, Class<T> clazz) {
        SequenceInfo sequenceInfo = loadFields(clazz);
        try {
            T target = clazz.newInstance();
            sequenceInfo.seqNameField.set(target, seqName);
            sequenceInfo.seqNextValField.set(target, seqNextVal);
            if(sequenceInfo.seqCategoryField != null) {
                sequenceInfo.seqCategoryField.set(target, seqCategory);
            }
            return target;
        } catch (Exception e) {
           throw new RuntimeException(e);
        }
    }


    static class SequenceInfo {
        protected Field seqNameField = null;
        protected String seqNameFieldName = null;
        protected Field seqNextValField = null;
        protected String seqNextValFieldName = null;
        protected Field seqCategoryField = null;
        protected String seqCategoryFieldName = null;
        protected String tableName = null;
        protected String selectSql = null;
        protected String updateSql = null;

        public SequenceInfo(Field seqNameField, String seqNameFieldName, Field seqNextValField, String seqNextValFieldName, Field seqCategoryField, String seqCategoryFieldName, String tableName, String selectSql, String updateSql) {
            this.seqNameField = seqNameField;
            this.seqNameFieldName = seqNameFieldName;
            this.seqNextValField = seqNextValField;
            this.seqNextValFieldName = seqNextValFieldName;
            this.seqCategoryField = seqCategoryField;
            this.seqCategoryFieldName = seqCategoryFieldName;
            this.tableName = tableName;
            this.selectSql = selectSql;
            this.updateSql = updateSql;
        }
    }

    static class NameAndValue {
        String seqName;
        Long seqVal;
        String category;

        public NameAndValue(String seqName, Long seqVal, String category) {
            this.seqName = seqName;
            this.seqVal = seqVal;
            this.category = category;
        }
    }
}
