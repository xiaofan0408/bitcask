package com.xiaofan0408.bitcask.v2.storage;

import com.xiaofan0408.bitcask.v2.utils.Crc32;
import com.xiaofan0408.bitcask.v2.utils.ByteUtils;
import lombok.Data;

import java.io.IOException;
import java.nio.ByteBuffer;

@Data
public class Entry {

    /**
     *  8 + 4 + 4 + 4 + 4 + 4
     */
    public static final int ENTRY_HEADER_SIZE = 28;

    public static final byte[] EMPTY_ARRAY = new byte[0];

    private Meta meta;

    private int type;

    private int mark;

    private long crc32;

    public Entry() {

    }


    public Entry(byte[] key,byte[] value ,byte[] extra,int type,int mark){
        this.meta = new Meta(key,value,extra,key.length,value.length,extra.length);
        this.type = type;
        this.mark = mark;
    }


    public Entry(byte[] key,byte[] value ,int type,int mark){
        this.meta = new Meta(key,value,EMPTY_ARRAY,key.length,value.length,0);
        this.type = type;
        this.mark = mark;
    }

    public int size() {
        return ENTRY_HEADER_SIZE + meta.getKeySize() + meta.getValueSize() + meta.getExtraSize();
    }

    public ByteBuffer encode() throws IOException {
        byte[] data = new byte[ENTRY_HEADER_SIZE];
        ByteBuffer h = ByteBuffer.wrap(data);
        h.putInt(8,type);
        h.putInt(12,mark);
        h.putInt(16, meta.getKeySize());
        h.putInt(20, meta.getValueSize());
        h.putInt(24, meta.getExtraSize());
        Crc32 crc = new Crc32();
        crc.update(data, 8, 20);
        crc.update(meta.getKey());
        crc.update(meta.getValue());
        crc.update(meta.getExtra());
        long crc32 = crc.getValue();
        h.putLong(0,crc32);
        byte[] result = ByteUtils.concat(data,meta.getKey(),meta.getValue(),meta.getExtra());
        return ByteBuffer.wrap(result);
    }

    public static Entry decode(ByteBuffer byteBuffer) {
        byteBuffer.rewind();
        long crc32 = byteBuffer.getLong();
        int type = byteBuffer.getInt();
        int mark = byteBuffer.getInt();
        int keySize = byteBuffer.getInt();
        int valueSize = byteBuffer.getInt();
        int extraSize = byteBuffer.getInt();
        byte[] keyArray = new byte[keySize];
        byte[] valueArray = new byte[valueSize];
        byte[] extraArray = new byte[extraSize];
        byteBuffer.get(keyArray);
        byteBuffer.get(valueArray);
        byteBuffer.get(extraArray);

        Meta meta = new Meta(keyArray,valueArray,extraArray,keySize,valueSize,extraSize);
        Entry entry = new Entry();
        entry.setMeta(meta);
        entry.setCrc32(crc32);
        entry.setType(type);
        entry.setMark(mark);
        return entry;
    }

}
