package com.maketubo.sequence;

import org.springframework.util.Assert;

import java.util.Arrays;

/**
 * @author maketubo
 * @version 1.0
 * @ClassName SeqName
 * @description
 * @date 2020/6/3 16:14
 * @since JDK 1.8
 */
public class SeqName {

    private long[] longs = new long[4];

    private byte[] bytes;

    public SeqName(String name){
        Assert.isTrue(name != null && name.length() != 0, "序列不允许为空");
        initLongs(name.getBytes());
    }

    public SeqName(byte[] bytes) {
        initLongs(bytes);
    }

    private void initLongs(byte[] bytes) {
        this.bytes = bytes;
        Assert.isTrue(bytes.length <= 32, "序列长度不允许超过32");
        for(int i = 0; i < bytes.length; i = i + 8) {
            byte[] array;
            if(bytes.length - i < 8){
                array = Arrays.copyOfRange(bytes, i, bytes.length);
            } else {
                array = Arrays.copyOfRange(bytes, i, i + 8);
            }
            longs[i >> 3] = getLongFromBytes(array);
        }
    }

    private long getLongFromBytes(byte[] array) {
        if(array.length == 0) return 0l;
        long result = ((long)array[0] & 255L) << 56;
        for(int i = 1; i <= array.length - 1; i++) {
            result = result | ((long)array[i] & 255L) << (2 << (7-i));
        }
        return result;
    }

    public long[] getLongs() {
        return longs;
    }

    public byte[] getBytes() {
        return bytes;
    }

    @Override
    public int hashCode(){
        return Arrays.hashCode(longs);
    };

    @Override
    public boolean equals(Object obj) {
        if(obj == null) return false;
        long[] compares = ((SeqName) obj).getLongs();
        return (this.longs[0] == compares[0])
                && (this.longs[1] == compares[1])
                && (this.longs[2] == compares[2])
                && (this.longs[3] == compares[3]);
    }


}
