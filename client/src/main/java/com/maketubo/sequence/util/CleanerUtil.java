package com.maketubo.sequence.util;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;

/**
 * 为适配不同版本java
 * @author maketubo
 * @version 1.0
 * @ClassName JavaVersionUtil
 * @description
 * @date 2020/6/1 9:41
 * @since JDK 1.8
 */
public class CleanerUtil {

    private static MethodHandle methodHandle;
    private static MethodHandle cleanerMethodHandle;
    private static MethodHandle clean;
    static {
        MethodHandles.Lookup lookup = MethodHandles.publicLookup();

        if (isJava9OrLater()) {
            // Java 9 branch
            // Unsafe.theUnsafe.invokeCleaner(byteBuffer)
            try {
                Class<?> unsafeClass = Class.forName("sun.misc.Unsafe");
                Field instanceField = unsafeClass.getDeclaredField("theUnsafe");
                if (!instanceField.isAccessible()) {
                    instanceField.setAccessible(true);
                }
                Object unsafe = instanceField.get(null);
                Method invokeCleaner = unsafeClass.getDeclaredMethod("invokeCleaner", ByteBuffer.class);
                methodHandle = lookup.unreflect(invokeCleaner).bindTo(unsafe);
            } catch (Exception e) {
                throw new IllegalStateException("could not get invokeCleaner method handle", e);
            } catch (Throwable throwable) {
                throwable.printStackTrace();
            }
        } else {
            // Java 8 branch
            // sun.misc.Cleaner cleaner = ((sun.nio.ch.DirectBuffer) buffer).cleaner();
            // cleaner.clean();
            try {
                Class<?> bufferInterface = Class.forName("java.nio.DirectByteBuffer");
                Method cleanerMethod = bufferInterface.getDeclaredMethod("cleaner");
                if (!cleanerMethod.isAccessible()) {
                    cleanerMethod.setAccessible(true);
                }
                Class<?> cleanerClass = Class.forName("sun.misc.Cleaner");
                Method cleanMethod = cleanerClass.getDeclaredMethod("clean"); // should be accessible
                cleanerMethodHandle = lookup.unreflect(cleanerMethod);
                clean = lookup.unreflect(cleanMethod);
            } catch (ReflectiveOperationException e) {
                throw new IllegalStateException("could not get cleaner clean method handle", e);
            } catch (Throwable throwable) {
                throwable.printStackTrace();
            }
        }
    }

    public static boolean isJava9OrLater() {
        try {
            Class.forName("java.lang.Runtime$Version");
            return true;
        } catch (ClassNotFoundException e) {
            return false;
        }
    }

    public static void clean(Object buffer) {
        try {
            if (isJava9OrLater()) {
                methodHandle.invokeExact((ByteBuffer)buffer);
            } else {
                clean.invoke(cleanerMethodHandle.invoke((ByteBuffer)buffer));
            }
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }

    }
}
