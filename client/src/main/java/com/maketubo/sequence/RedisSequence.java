package com.maketubo.sequence;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.ReturnType;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.serializer.RedisSerializer;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 落库是为了保证持久化
 * 从redis获取seq.next是为了满足高并发
 * 使用zookeeper是为了保证分布式节点的local cache协调一致 保证redis宕机丢失seq value后seq的严格正确
 * @author maketubo
 * @version 1.0
 * @ClassName RedisSequence
 * @description
 * @date 2020/5/30 19:26
 * @since JDK 1.8
 */
public class RedisSequence extends AbstractSequence {

    private Logger logger = LoggerFactory.getLogger(RedisSequence.class);

    private Set<String> seqNames = ConcurrentHashMap.newKeySet();

    private RedisTemplate<String, String> redisTemplate;

    private String seqDirKey;

    public RedisSequence(RedisTemplate redisTemplate, String seqDirKey){
        this.redisTemplate = redisTemplate;
        this.seqDirKey = seqDirKey;
    }

    public RedisSequence(RedisTemplate redisTemplate, String seqDirKey, List<?> externalList){
        this.redisTemplate = redisTemplate;
        this.seqDirKey = seqDirKey;
        if(externalList != null && !externalList.isEmpty()){
            loadSequences(externalList);
        }
    }

    public Long getNext(String seqName, int num, boolean createWhenEmpty) {
        return redisTemplate.execute(new DefaultRedisScript<>(Script.GET_AND_SET_SCRIPT, Long.class),
                Arrays.asList(seqDirKey), seqName, String.valueOf(num), String.valueOf(createWhenEmpty));
    }

    protected void loadSequences(List<?> externalList) {
        RedisSerializer stringSerializer = redisTemplate.getStringSerializer();
        List<?> sequences = externalList;
        try {
            Map<String, Long> sequenceMap = SequenceManager.getSequenceMap(sequences);
            if (sequenceMap.entrySet().stream().anyMatch(f -> f.getValue() == null || f.getValue() < 0l)) {
                logger.warn("seq init with error value");
                return;
            }
            seqNames.addAll(sequenceMap.keySet());
            byte[][] bytes = new byte[sequenceMap.size() * 2 + 1][];
            int num = 0;
            bytes[num] = stringSerializer.serialize(seqDirKey);
            for (Map.Entry entry : sequenceMap.entrySet()) {
                bytes[++num] = stringSerializer.serialize(entry.getKey());
                bytes[++num] = stringSerializer.serialize(String.valueOf(entry.getValue()));
            }
            RedisConnection connection = redisTemplate.getConnectionFactory().getConnection();
            Object eval = connection.scriptingCommands().eval(stringSerializer.serialize(Script.INIT_SCRIPT),
                    ReturnType.INTEGER, 1, bytes);
            if (!eval.equals(1l)) {
                logger.warn("eval script error please check!");
            }
        } catch (Exception e) {
            //do nothing
            logger.warn(e.getMessage());
        }
    }

    class Script {
        private static final String INIT_SCRIPT =
                "local array = ARGV\n" +
                        "local newarray = {}\n" +
                        "for i=1,#array do\n" +
                        "   if (i%2 ~= 0) then\n" +
                        "    local v = redis.call('hget', KEYS[1], array[i])\n" +
                        "    if ((v == nil or (type(v) == 'boolean' and not(v))) or v < array[i+1]) then\n" +
                        "     table.insert(newarray, array[i])\n" +
                        "     table.insert(newarray, array[i+1])\n" +
                        "    end\n" +
                        "   end\n" +
                        "end\n" +
                        "redis.call('hmset',KEYS[1],unpack(newarray))\n" +
                        "return 1";

        private static final String GET_AND_SET_SCRIPT =
                "local key = KEYS[1]\n" +
                        "local hash = ARGV[1]\n" +
                        "local incrNum = ARGV[2]\n" +
                        "local exists = redis.call('HEXISTS', key, hash)\n" +
                        "if (exists == 1 and ARGV[3] == 'false') then\n" +
                        "     return nil\n" +
                        "end\n" +
                        "return redis.call(\"HINCRBY\", key, hash, incrNum)";

        private static final String GET_ALL_SEQ_SCRIPT = "return redis.call(\"HGETALL\", KEYS[1])";
    }

}
