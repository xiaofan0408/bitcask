package com.xiaofan0408.bitcask.common.my.utils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * @author xuzefan  2020/7/6 10:54
 */
public class ByteUtils {

    public static byte[] concat(byte[]... arrays) throws IOException {
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        for (byte[] arr: arrays) {
            os.write(arr);
        }
        byte[] byteArray = os.toByteArray();
        return byteArray;
    }
}
