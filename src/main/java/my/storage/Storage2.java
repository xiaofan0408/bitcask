package my.storage;

import my.entity.Index;
import my.entity.Item;
import my.utils.ByteUtils;
import my.utils.Crc32;
import my.utils.FileUtils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * @author xuzefan  2019/5/10 14:07
 */
public class Storage2 {

    // 8 + 8 + 4 + 4
    public static int HEADER_SIZE = 24;

    private final int THRESHOLD = 1024 * 1024 * 1024;

    private final String basePath = "./data";

    private AtomicInteger currentActive = new AtomicInteger(0);

    private FileChannel writeChannel;

    private FileChannel readChannel;

    private Map<String,FileChannel> readChannelMap = new ConcurrentHashMap<>();

    private Map<String,byte[]> dataMap = new ConcurrentHashMap<>();

    // 读写缓冲区
    private ByteBuffer buffer = ByteBuffer.allocateDirect(64 * 1024 * 1024);

    private static ExecutorService flushThread = Executors.newSingleThreadExecutor();

    private static ByteBuffer flushBuffer = ByteBuffer.allocateDirect(64 * 1024 * 1024);

    private Future<Long> flushFuture;

    public Storage2() {
        initCurrentActive();
        initAccessFile();
    }

    public void loadData(KeyDir keyDir) {
        try {
            List<File> files = FileUtils.listFile(basePath);
            files.forEach( file -> {
                try {
                    FileChannel channel;
                    if (this.readChannelMap.get(file)!=null){
                        channel = this.readChannelMap.get(file);
                    } else {
                        channel = new RandomAccessFile(file,"r").getChannel();
                    }
                    while (channel.size() != channel.position()) {
                        readOneIndex(channel,file.getPath(),keyDir);
                    }
                    channel.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    private long length(ByteBuffer[] vec) {
        long length = 0;
        for (int i = 0; i < vec.length; i++) {
            length += vec[i].remaining();
        }
        return length;
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
            if ( this.writeChannel.size() > THRESHOLD){
                currentActive.incrementAndGet();
                String activeFileName = basePath + "/" + String.format("%s.sst",currentActive);
                File file = new File(activeFileName);
                if (!file.exists()) {
                    file.createNewFile();
                }
                this.writeChannel = new RandomAccessFile(file,"rw").getChannel();
                this.readChannel = new RandomAccessFile(file,"r").getChannel();
            }
            return this.writeChannel;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public Index ioWrite(long ts,int key_size,int value_size,byte[] keyArray,byte[] valueArray){
        try {
            FileChannel fileChannel = this.getWriteAccessFile();
            fileChannel.position(fileChannel.size());
            long start = fileChannel.position();
            long length = HEADER_SIZE + key_size + value_size;
            if (length > buffer.remaining()) {
                // 落盘
                flush();
            }
            byte[] message = getBytes(ts,key_size,value_size,keyArray,valueArray);
            buffer.put(message);
            String indexName = basePath + "/"+ String.format("%s.sst_%d_%d",currentActive,start,length);
            dataMap.put(indexName,message);
            return new Index(basePath + "/"+ String.format("%s.sst",currentActive),start,length);
        }catch (Exception e){}
        return null;
    }

    private void flush() {
        if (flushFuture != null) {
            try {
                flushFuture.get();
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
            flushFuture = null;
        }

        buffer.flip();
        int remaining = buffer.remaining();
        final byte[] message = new byte[remaining];
        buffer.get(message);
        flushFuture = flushThread.submit(new Callable<Long>() {
            @Override
            public Long call() throws Exception {
                try {
                    if (flushBuffer.remaining() < message.length) {
                        FileChannel fileChannel = null;
                        if (writeChannel.size() + flushBuffer.remaining()> THRESHOLD) {
                            currentActive.incrementAndGet();
                            String activeFileName = basePath + "/" + String.format("%s.sst",currentActive);
                            File file = new File(activeFileName);
                            if (!file.exists()) {
                                file.createNewFile();
                            }
                            writeChannel = new RandomAccessFile(file,"rw").getChannel();
                            readChannel = new RandomAccessFile(file,"r").getChannel();
                        } else {
                            fileChannel = writeChannel;
                        }
                        flushBuffer.flip();
                        fileChannel.write(flushBuffer);
                        flushBuffer.clear();
                        dataMap.clear();
                    }
                    flushBuffer.put(message);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                return 0L;
            }
        });
        buffer.clear();
    }

    private long writeFully(FileChannel ch, ByteBuffer vec) throws IOException {
       return ch.write(vec);
    }

    public Item ioRead(Index info) {
        String indexName = info.getActiveFileName() + "_" + info.getStart() + "_" + info.getLength();
        if (dataMap.get(indexName) != null) {
            ByteBuffer byteBuffer = ByteBuffer.wrap(dataMap.get(indexName));
            return byteBufferToItem(byteBuffer);
        }
        FileChannel fileChannel = this.getReadAccessFile(info);
        try {
            byte[] byteArray = new byte[(int)info.getLength()];
            ByteBuffer byteBuffer = ByteBuffer.wrap(byteArray);
            fileChannel.position(info.getStart());
            fileChannel.read(byteBuffer);
            return byteBufferToItem(byteBuffer);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    private Item byteBufferToItem(ByteBuffer byteBuffer) {
        byteBuffer.rewind();
        long crc32 = byteBuffer.getLong();
        long ts = byteBuffer.getLong();
        int key_size = byteBuffer.getInt();
        int value_size = byteBuffer.getInt();
        byte[] keyArray = new byte[key_size];
        byte[] valueArray = new byte[value_size];
        byteBuffer.get(keyArray);
        byteBuffer.get(valueArray);
        return getItem(crc32,ts,key_size,value_size,keyArray,valueArray);
    }

    private byte[] getBytes(long ts, int key_size, int value_size, byte[] keyArray, byte[] valueArray) throws IOException {
        byte[] data = new byte[HEADER_SIZE];
        ByteBuffer h = ByteBuffer.wrap(data);
        h.putLong(8,ts);
        h.putInt(16,key_size);
        h.putInt(20,value_size);
        Crc32 crc = new Crc32();
        crc.update(data, 8, 16);
        crc.update(keyArray);
        crc.update(valueArray);
        long crc32 = crc.getValue();
        h.putLong(0,crc32);
        byte[] result = ByteUtils.concat(data,keyArray,valueArray);
        return result;
    }

    private ByteBuffer getByteBuffer(long ts, int key_size, int value_size, byte[] keyArray, byte[] valueArray) throws IOException {
        byte[] data = new byte[HEADER_SIZE];
        ByteBuffer h = ByteBuffer.wrap(data);
        h.putLong(8,ts);
        h.putInt(16,key_size);
        h.putInt(20,value_size);
        Crc32 crc = new Crc32();
        crc.update(data, 8, 16);
        crc.update(keyArray);
        crc.update(valueArray);
        long crc32 = crc.getValue();
        h.putLong(0,crc32);
        byte[] result = ByteUtils.concat(data,keyArray,valueArray);
        return ByteBuffer.wrap(result);
    }

    private void readOneIndex(FileChannel channel,String path,KeyDir keyDir) throws IOException {
        long start = channel.position();
        byte[] bytes = new byte[HEADER_SIZE];
        ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
        channel.read(byteBuffer);
        byteBuffer.rewind();
        long crc32 = byteBuffer.getLong();
        long ts = byteBuffer.getLong();
        int key_size = byteBuffer.getInt();
        int value_size = byteBuffer.getInt();
        byte[] keyArray = new byte[key_size];
        ByteBuffer k = ByteBuffer.wrap(keyArray);
        channel.read(k);
        String key = new String(keyArray, Charset.forName("utf-8"));
        keyDir.put(key,new Index(path ,start, HEADER_SIZE + key_size + value_size));
        long end = start + HEADER_SIZE + key_size + value_size;
        channel.position(end);
    }

    private Item getItem(long crc32, long ts, int key_size, int value_size, byte[] keyArray, byte[] valueArray){
        String key = new String(keyArray,Charset.forName("utf-8"));
        String value = new String(valueArray,Charset.forName("utf-8"));
        return new Item(crc32,ts,key_size,value_size,key,value);
    }

    public static byte[] copyOf(byte[] original, int newLength) {
        byte[] copy = new byte[newLength];
        System.arraycopy(original, 0, copy, 0,
                Math.min(original.length, newLength));
        return copy;
    }

}
