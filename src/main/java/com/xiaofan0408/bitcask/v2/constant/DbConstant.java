package com.xiaofan0408.bitcask.v2.constant;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class DbConstant {

    public static Integer STRING = 1;

    public static Integer LIST =  2;

    public static Integer HASH = 3;

    public static Integer SET = 4;

    public static Integer ZSET = 5;

    public static Map<Integer,String> DBFileFormatNames = new ConcurrentHashMap<>();

    public static Map<Integer,String> DBFileSuffixName = new ConcurrentHashMap<>();

    static {
        DBFileFormatNames.put(STRING,"%09d.data.str");
        DBFileFormatNames.put(LIST,"%09d.data.list");
        DBFileFormatNames.put(HASH,"%09d.data.hash");
        DBFileFormatNames.put(SET,"%09d.data.set");
        DBFileFormatNames.put(ZSET,"%09d.data.zset");


        DBFileSuffixName.put(STRING,"str");
        DBFileSuffixName.put(LIST,"list");
        DBFileSuffixName.put(HASH,"hash");
        DBFileSuffixName.put(SET,"set");
        DBFileSuffixName.put(ZSET,"zset");

    }
}
