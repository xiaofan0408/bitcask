package com.xiaofan0408.bitcask.common.my.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;

import java.io.Serializable;

/**
 * @author xuzefan  2019/5/4 17:55
 */
@Data
@AllArgsConstructor
public class Item implements Serializable {

    private long crc32;

    private long ts;

    private int key_size;

    private int value_size;

    private String key;

    private String value;
}
