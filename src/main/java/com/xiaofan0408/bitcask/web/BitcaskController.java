package com.xiaofan0408.bitcask.web;

import com.xiaofan0408.bitcask.model.PutDTO;
import com.xiaofan0408.bitcask.service.BitcaskService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

@RestController
public class BitcaskController {

    @Autowired
    private BitcaskService bitcaskService;

    @PostMapping("/put")
    public boolean put(@RequestBody PutDTO putDTO){
       return bitcaskService.put(putDTO);
    }

    @PostMapping("/put_async")
    public boolean putAsync(@RequestBody PutDTO putDTO){
       return bitcaskService.putAsync(putDTO);
    }

    @GetMapping("/get")
    public String get(String key) throws Exception {
        return bitcaskService.get(key);
    }

}
