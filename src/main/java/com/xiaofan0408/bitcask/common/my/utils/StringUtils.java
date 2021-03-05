package com.xiaofan0408.bitcask.common.my.utils;

import java.io.UnsupportedEncodingException;

public class StringUtils {

    public static byte[] getStringByte(String data){
        try {
            return data.getBytes("utf-8");
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return null;
    }

}
