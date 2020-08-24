package com.maketubo.sequence;

import com.maketubo.sequence.util.ByteUtil;
import com.maketubo.sequence.util.CleanerUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.SYNC;
import static java.nio.file.StandardOpenOption.WRITE;

/**
 *
 * @author maketubo
 * @version 1.0
 * @ClassName LocalSequence
 * @description
 * @date 2020/6/1 10:38
 * @since JDK 1.8
 */
public class LocalSequence extends AbstractSequence {

    private Logger logger = LoggerFactory.getLogger(LocalSequence.class);

    private static final String BLOOM_FILTER_PATH = "/app/bloomFilter.data";

    private static final String DEFAULT_INDEX_FILE_PATH = "/app/default/seq.index";

    private static final String DEFAULT_DATA_FILE_PATH = "/app/default/seq.data";

    private String indexFilePath;
    //后期版本考虑去掉 dataFile里面的seqName
    private String dataFilePath;

    private MappedByteBuffer indexBuffer;
    private int indexSize = 0;
    private MappedByteBuffer dataBuffer;
    private int dataSize = 0;

    private ReentrantLock lock = new ReentrantLock();

    public LocalSequence(Object category, List<?> externalList) {
        if(category == null) {
            init(DEFAULT_INDEX_FILE_PATH, DEFAULT_DATA_FILE_PATH, externalList);
        } else {
            init(String.format("/app/%s/seq.index", category),
                    String.format("/app/%s/seq.data", category),
                    externalList);
        }
    }

    public <T> LocalSequence(String indexFilePath, String dataFilePath, List<T> externalList) {
        init(indexFilePath, dataFilePath, externalList);
    }

    public <T>  void init(String indexFilePath, String dataFilePath, List<T> externalList) {
        this.indexFilePath = indexFilePath;
        this.dataFilePath = dataFilePath;
        //加载布隆过滤器
        //加载堆外内存
        if(externalList != null && !externalList.isEmpty()){
            loadSequences(externalList);
        }
    }

    // index 现在顺序排列 后面改为二叉树
    protected void loadSequences(List<?> externalList) {
        File indexFile = new File(indexFilePath); File dataFile = new File(dataFilePath);
        boolean exists = dataFile.exists();
        Map<String, Long> sequenceMap = SequenceManager.getSequenceMap(externalList);
        if(exists) {
            dataBuffer = initBuf(dataFile, 1024 * 100);
            //先从索引文件里面找
            indexBuffer = initBuf(indexFile, 1024 * 100);
            Map<SeqName, Long> remoteSeqMap = sequenceMap.entrySet().stream()
                    .collect(Collectors.toMap(entry ->
                                    new SeqName(entry.getKey().getBytes(Charset.forName("UTF-8"))),
                            entry -> entry.getValue(), (a1, a2) -> a2));
            byte[] nameBytes = new byte[32];
            byte[] offsetBytes = new byte[4];
            byte[] valBytes = new byte[8];
            while (indexBuffer.remaining() >= 32 && dataBuffer.remaining() >= 40){
                indexSize = indexBuffer.position();
                dataSize = dataBuffer.position();
                indexBuffer.get(nameBytes);
                indexBuffer.get(offsetBytes);
                if(ByteUtil.isZero(nameBytes)) {
                    break;
                }
                SeqName seqName = new SeqName(nameBytes);
                Long remoteVal = remoteSeqMap.get(seqName);
                if(remoteVal != null) {
                    int offset = ByteUtil.convertByteToInt(offsetBytes);
                    dataBuffer.position(offset);
                    dataBuffer.get(valBytes);
                    long localVal = ByteUtil.convertByteToLong(valBytes);
                    if(remoteVal != null && remoteVal > localVal) {
                        dataBuffer.position(offset);
                        dataBuffer.put(valBytes);
                    }
                    sequenceMap.remove(new String(seqName.getBytes()));
                }
            }
            if(!remoteSeqMap.isEmpty()) {
                //append to exists file
                install(sequenceMap);
            }
        } else {
            install(sequenceMap);
        }
    }

    private void install(Map<String, Long> sequenceMap) {
        prepareBufferIfEmpty();
        int i = 0;
        indexBuffer.position(indexSize);
        dataBuffer.position(dataSize);
        for (Map.Entry<String, Long> entry : sequenceMap.entrySet()) {
            byte[] seqNameBytes = entry.getKey().getBytes(Charset.forName("UTF-8")); //32
            if (seqNameBytes.length < 32) {
                byte[] zeroBytes = new byte[32 - seqNameBytes.length];
                seqNameBytes = mergeByteArray(seqNameBytes, zeroBytes);
            }
            indexBuffer.put(seqNameBytes);
            dataBuffer.put(seqNameBytes);
            indexBuffer.put(ByteUtil.convertIntToByte(32 + 40 * i));
            byte[] seqValBytes = ByteUtil.convertLongToByte(entry.getValue()); //8
            dataBuffer.put(seqValBytes);
            i++;
        }
        indexSize = indexBuffer.position();
        dataSize = dataBuffer.position();
    }

    private void install(String seqName, Long seqVal) {
        prepareBufferIfEmpty();
        indexBuffer.position(indexSize);
        dataBuffer.position(dataSize);
        byte[] seqNameBytes = seqName.getBytes(Charset.forName("UTF-8")); //32
        if (seqNameBytes.length < 32) {
            byte[] zeroBytes = new byte[32 - seqNameBytes.length];
            seqNameBytes = mergeByteArray(seqNameBytes, zeroBytes);
        }
        indexBuffer.put(seqNameBytes);
        dataBuffer.put(seqNameBytes);
        indexBuffer.put(ByteUtil.convertIntToByte(dataSize + 32));
        byte[] seqValBytes = ByteUtil.convertLongToByte(seqVal); //8
        dataBuffer.put(seqValBytes);
        indexSize = indexBuffer.position();
        dataSize = dataBuffer.position();
    }

    @Override
    public Long getNext(String seqName, int num, boolean createWhenEmpty) {
        // not init with externalList
        prepareBufferIfEmpty();
        lock.lock();
        try {
            //布隆过滤器判断是否seqName在文件中
            //不在文件中直接返回 -1
            indexBuffer.position(0);
            byte[] nameBytes = new byte[32];
            byte[] offsetBytes = new byte[4];
            byte[] valBytes = new byte[8];
            while (indexBuffer.remaining() >= 32 && dataBuffer.remaining() >= 40){
                indexBuffer.get(nameBytes);
                indexBuffer.get(offsetBytes);
                if(ByteUtil.isZero(nameBytes)) {
                    break;
                }
                int offset = ByteUtil.convertByteToInt(offsetBytes);
                dataBuffer.position(offset);
                dataBuffer.get(valBytes);
                long localVal = ByteUtil.convertByteToLong(valBytes);
                if(compareIgnoreLen(nameBytes, seqName.getBytes()) == 0){
                    long result = localVal + num;
                    dataBuffer.position(offset);
                    dataBuffer.put(ByteUtil.convertLongToByte(result));
                    return result;
                }
            }
            if(createWhenEmpty) {
                install(seqName, 0l);
                return 0l;
            }
            return null;
        } finally {
            lock.unlock();
        }
    }

    private void prepareBufferIfEmpty() {
        if(dataBuffer == null) {
            dataBuffer = initBuf(new File(dataFilePath), 1024 * 100);
            indexBuffer = initBuf(new File(indexFilePath), 1024 * 100);
            dataBuffer.position(0);
            indexBuffer.position(0);
        }
    }

    private MappedByteBuffer initBuf(File file, long size) {
        //判断目标文件所在的目录是否存在
        if(!file.getParentFile().exists()) {
            //如果目标文件所在的目录不存在，则创建父目录
            logger.debug("目标文件所在目录不存在，准备创建它！");
            if(!file.getParentFile().mkdirs()) {
                throw new RuntimeException("创建目标文件所在目录失败！");
            }
        }
        //把文件内容映射到堆外内存上
        try (FileChannel channel = FileChannel.open(Paths.get(file.getAbsolutePath()),
                READ, WRITE, CREATE, SYNC)){
            MappedByteBuffer map = channel.map(FileChannel.MapMode.READ_WRITE, 0, size);
            return map;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    protected void finalize() throws Throwable {
        try {
            // finally
            if (dataBuffer != null) {
                CleanerUtil.clean(dataBuffer);
            }
            if (indexBuffer != null) {
                CleanerUtil.clean(indexBuffer);
            }
        } finally {
            super.finalize();
        }
    }

    private byte[] mergeByteArray(byte[] remain, byte[] bytes){
        return mergeByteArray(remain, bytes, 0, bytes.length);
    }

    private byte[] mergeByteArray(byte[] remain, byte[] bytes, int from, int to){
        byte[] result;
        if(remain == null) {
            result =  new byte[to - from];
            System.arraycopy(bytes, from, result, 0, result.length);
        } else {
            result =  new byte[remain.length + to - from];
            System.arraycopy(remain, 0, result, 0, remain.length);
            System.arraycopy(bytes, from, result, remain.length, to - from);
        }
        return result;
    }

    public static int compareIgnoreLen(byte[] a, byte[] b)
    {
        if (a == b)
        {
            return 0;
        }
        if (a == null)
        {
            return -1;
        }
        if (b == null)
        {
            return 1;
        }
        int minLen = Math.min(a.length, b.length);
        for (int i = 0; i < minLen; ++i)
        {
            int aVal = a[i] & 0xFF, bVal = b[i] & 0xFF;
            if (aVal < bVal)
            {
                return -1;
            }
            if (aVal > bVal)
            {
                return 1;
            }
        }
        return 0;
    }

    public void writeVal(String seqName, Long currentVal) {
        lock.lock();
        try {
            //布隆过滤器判断是否seqName在文件中
            //不在文件中直接返回 -1 然后写入最后
            indexBuffer.position(0);
            byte[] nameBytes = new byte[32];
            byte[] offsetBytes = new byte[4];
            byte[] oldValBytes = new byte[8];
            boolean find = false;
            while (indexBuffer.remaining() >= 32 && dataBuffer.remaining() >= 40){
                indexBuffer.get(nameBytes);
                indexBuffer.get(offsetBytes);
                int offset = ByteUtil.convertByteToInt(offsetBytes);
                if(compareIgnoreLen(nameBytes, seqName.getBytes()) == 0){
                    find = true;
                    dataBuffer.position(offset);
                    dataBuffer.get(oldValBytes);
                    if (ByteUtil.convertByteToLong(oldValBytes) > currentVal){
                        //ignore
                    } else {
                        dataBuffer.position(offset);
                        dataBuffer.put(ByteUtil.convertLongToByte(currentVal));
                    }
                }
            }
            if(!find) {
//                int indexOffset = (seqSize - 1) * 40;
//                int dataOffset = (seqSize - 1) * 36;
                int indexOffset = 40;
                int dataOffset = 36;
                byte[] seqNameBytes = seqName.getBytes();
                if (seqNameBytes.length < 32) {
                    byte[] zeroBytes = new byte[32 - seqNameBytes.length];
                    seqNameBytes = mergeByteArray(seqNameBytes, zeroBytes);
                }
                indexBuffer.position(indexOffset);
                indexBuffer.put(seqNameBytes);
                indexBuffer.put(ByteUtil.convertIntToByte(dataOffset));
                dataBuffer.position(dataOffset);
                dataBuffer.put(seqNameBytes);
                dataBuffer.put(ByteUtil.convertLongToByte(currentVal));
            }
        } finally {
            lock.unlock();
        }
    }

    public Long getCurrentVal(String seqName) {
        lock.lock();//todo read write lock
        try {
            //布隆过滤器判断是否seqName在文件中
            //不在文件中直接返回 -1
            indexBuffer.position(0);
            byte[] nameBytes = new byte[32];
            byte[] offsetBytes = new byte[4];
            byte[] valBytes = new byte[8];
            while (indexBuffer.remaining() >= 32 && dataBuffer.remaining() >= 40){
                indexBuffer.get(nameBytes);
                indexBuffer.get(offsetBytes);
                int offset = ByteUtil.convertByteToInt(offsetBytes);
                dataBuffer.position(offset);
                dataBuffer.get(valBytes);
                long localVal = ByteUtil.convertByteToLong(valBytes);
                if(compareIgnoreLen(nameBytes, seqName.getBytes()) == 0){
                    long result = localVal;
                    return result;
                }
            }
            return null;
        } finally {
            lock.unlock();
        }
    }
}
