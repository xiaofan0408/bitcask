package com.xiaofan0408.bitcask.v1.common.my.entity;


import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class Index {

    private String activeFileName;

    private long start;

    private long length;

}
