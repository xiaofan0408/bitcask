package com.xiaofan0408.bitcask.v2.storage.impl;

import com.xiaofan0408.bitcask.v1.common.my.entity.Index;
import com.xiaofan0408.bitcask.v1.common.my.utils.FileUtils;
import com.xiaofan0408.bitcask.v2.storage.Entry;
import com.xiaofan0408.bitcask.v2.storage.Storage;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;

public class FileChannelStorage implements Storage {

    private String basePath;

    private Map<Integer,FileChannel> writeChannelTypeMap = new ConcurrentHashMap<>();

    private Map<Integer,FileChannel> readChannelTypeMap = new ConcurrentHashMap<>();

    private Map<Integer,Map<String,FileChannel>> readChannelListMap = new ConcurrentHashMap<>();

    private Map<Integer, LongAdder> writePositionMap = new ConcurrentHashMap<>();

    private Map<Integer,LongAdder>  currentActive = new ConcurrentHashMap<>();

    private int thresHold = 1024 * 1024 * 1024;

    public FileChannelStorage(String basePath) {
        this.basePath = basePath;
    }

    public FileChannelStorage(String basePath,int blockSize) {
        this.basePath = basePath;
        this.thresHold = blockSize;
    }


    private void initCurrentActive(){
        try {
            List<String> fileNameList = FileUtils.getFile(basePath);
            if (fileNameList.size() == 0) {
                currentActive.set(1);
            } else {
                List<Integer> nameInteger = fileNameList.parallelStream().map( file -> {
                    return Integer.valueOf(file.replace(".sst",""));
                }).collect(Collectors.toList());
                currentActive.set(nameInteger.parallelStream().max(Integer::compare).get());
            }
        } catch (Exception e){
            e.printStackTrace();
        }
    }

    private void initAccessFile(){
        String activeFileName = basePath + "/" + String.format("%s.sst",currentActive);
        File file = new File(activeFileName);
        try{
            if (!file.exists()) {
                file.createNewFile();
            }
            this.readChannel = new RandomAccessFile(file,"r").getChannel();
            this.writeChannel = new RandomAccessFile(file,"rw").getChannel();
            this.writePosition = new AtomicLong(writeChannel.position());
            this.readChannelMap.put(activeFileName,readChannel);
            FileUtils.listFile(basePath).stream().forEach(f ->{
                try {
                    this.readChannelMap.put(f.getPath(),new RandomAccessFile(f,"r").getChannel());
                } catch (FileNotFoundException e) {
                    e.printStackTrace();
                }
            });
        }catch (Exception e){
            e.printStackTrace();
        }

    }

    private FileChannel getReadAccessFile(Index info){
        String activeFileName = basePath + "/" + String.format("%s.sst",currentActive);
        if (info.getActiveFileName().equals(activeFileName)) {
            return this.readChannel;
        } else {
            File file = new File(info.getActiveFileName());
            try {
                return  new RandomAccessFile(file,"r").getChannel();
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
        }
        return null;
    }

    private FileChannel getWriteAccessFile(){
        try {
            if ( this.writePosition.get()> THRESHOLD){
                currentActive.incrementAndGet();
                String activeFileName = basePath + "/" + String.format("%s.sst",currentActive);
                File file = new File(activeFileName);
                if (!file.exists()) {
                    file.createNewFile();
                }
                this.writeChannel = new RandomAccessFile(file,"rw").getChannel();
                this.readChannel = new RandomAccessFile(file,"r").getChannel();
                this.writePosition.set(0);
            }
            return this.writeChannel;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public Entry read(long offset) {

    }

    @Override
    public void write(Entry entry) {

    }

    @Override
    public void close(boolean sync) {

    }

    @Override
    public void sync() {

    }
}
