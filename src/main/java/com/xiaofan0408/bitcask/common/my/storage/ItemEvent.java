package com.xiaofan0408.bitcask.common.my.storage;

import lombok.Data;

@Data
public class ItemEvent {

    private String key;

    private long ts;

    private int keySize;

    private int valueSize;

    private byte[] keyArray;

    private byte[] valueArray;
}
