package com.maketubo.sequence.util;

import org.springframework.util.Assert;

/**
 * @author maketubo
 * @version 1.0
 * @ClassName ByteUtil
 * @description
 * @date 2020/8/15 18:36
 * @since JDK 1.8
 */
public class ByteUtil {

    public static int convertByteToInt(byte[] bytes) {
        Assert.isTrue(bytes.length == 4, "array length must be 4");
        return convertByteToInt(bytes[0], bytes[1], bytes[2], bytes[3]);
    }

    public static int convertByteToInt(byte b1, byte b2, byte b3, byte b4) {
        return b1 << 24 | (b2 & 255) << 16 | (b3 & 255) << 8 | b4 & 255;
    }

    public static long convertByteToLong(byte[] bytes) {
        Assert.isTrue(bytes.length == 8, "array length must be 8");
        return convertByteToLong(bytes[0], bytes[1], bytes[2], bytes[3],
                bytes[4], bytes[5], bytes[6], bytes[7]);
    }

    public static long convertByteToLong(byte b1, byte b2, byte b3, byte b4, byte b5, byte b6, byte b7, byte b8) {
        return ((long)b1 & 255L) << 56 | ((long)b2 & 255L) << 48 | ((long)b3 & 255L) << 40 | ((long)b4 & 255L) << 32 | ((long)b5 & 255L) << 24 | ((long)b6 & 255L) << 16 | ((long)b7 & 255L) << 8 | (long)b8 & 255L;
    }

    public static byte[] convertIntToByte(int value) {
        return new byte[] {
                (byte) (value >> 24), (byte) (value >> 16), (byte) (value >> 8), (byte) value
        };
    }

    public static byte[] convertLongToByte(long value) {
        byte[] result = new byte[8];
        for (int i = 7; i >= 0; i--) {
            result[i] = (byte) (value & 0xffL);
            value >>= 8;
        }
        return result;
    }

    public static boolean isZero(byte[] bytes){
        int sum = 0;
        for (byte b : bytes) {
            sum |= b;
        }
        return (sum == 0);
    }

}
