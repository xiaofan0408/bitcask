package com.xiaofan0408.bitcask;


import com.xiaofan0408.bitcask.common.my.utils.ByteUtils;
import com.xiaofan0408.bitcask.common.my.utils.Crc32;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * @author xuzefan  2020/7/6 10:20
 */
public class ByteTest {

    @Test
    public void test1(){
        byte[] data = new byte[20];
        ByteBuffer h = ByteBuffer.wrap(data);
        h.putLong(4,1);
        h.putInt(12,1);
        h.putInt(16,1);
        ByteBuffer key = ByteBuffer.wrap(new byte[128]);
        ByteBuffer value = ByteBuffer.wrap(new byte[128]);
        Crc32 crc = new Crc32();
        crc.update(data, 4, 16);
        crc.update(key);
        crc.update(value);
        int crc32 = crc.getValue();
        h.putInt(0,crc32);
        System.out.println(data);
    }

    @Test
    public void test2() throws IOException {
        byte[] data = new byte[20];
        ByteBuffer h = ByteBuffer.wrap(data);
        h.putLong(4,1);
        h.putInt(12,1);
        h.putInt(16,1);
        ByteBuffer key = ByteBuffer.wrap(new byte[128]);
        ByteBuffer value = ByteBuffer.wrap(new byte[128]);
        Crc32 crc = new Crc32();
        crc.update(data, 4, 16);
        crc.update(key);
        crc.update(value);
        int crc32 = crc.getValue();
        h.putInt(0,crc32);
        byte[] result = ByteUtils.concat(data, new byte[128],new byte[128]);
        System.out.println(result);
    }

}
