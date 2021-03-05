package com.xiaofan0408.bitcask.service;


import com.xiaofan0408.bitcask.common.my.core.Bitcask;
import com.xiaofan0408.bitcask.common.my.utils.ByteUtils;
import com.xiaofan0408.bitcask.common.my.utils.StringUtils;
import com.xiaofan0408.bitcask.model.PutDTO;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class BitcaskService {

    @Autowired
    private Bitcask bitcask;


    public boolean put(PutDTO putDTO) {
        bitcask.put(putDTO.getKey(),putDTO.getValue());
        return true;
    }

    public boolean putAsync(PutDTO putDTO) {
        bitcask.putAsync(putDTO.getKey(), StringUtils.getStringByte(putDTO.getValue()));
        return true;
    }

    public String get(String key) throws Exception {
       return bitcask.get(key);
    }
}
