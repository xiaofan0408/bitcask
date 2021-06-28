package com.xiaofan0408.bitcask.v1.model;

import lombok.Data;

@Data
public class PutDTO {

    private String key;

    private String value;
}
