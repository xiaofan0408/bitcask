package com.xiaofan0408.bitcask.v2.storage;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Meta {

    private byte[] key;

    private byte[] value;

    private byte[] extra;

    private int keySize;

    private int valueSize;

    private int extraSize;
}
