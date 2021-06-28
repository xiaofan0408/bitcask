package com.xiaofan0408.bitcask.v1.common.config;

import com.xiaofan0408.bitcask.v1.common.my.core.Bitcask;
import com.xiaofan0408.bitcask.v1.common.my.storage.impl.FileChannelStorage;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class BitcaskConfig {

    @Bean
    public Bitcask bitcask(){
        Bitcask bitcask = Bitcask.builder()
                .storage(new FileChannelStorage())
                .build();
        return bitcask;
    }

}
