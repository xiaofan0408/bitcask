package com.xiaofan0408.bitcask.common.my.utils;

import java.util.zip.CRC32;

/**
 * @author xuzefan  2019/5/4 17:47
 */
public class CrcUtil {

    public static long getCrc32(String value) {
        CRC32 crc32 = new CRC32();
        crc32.update(value.getBytes());
        return crc32.getValue();
    }

}
