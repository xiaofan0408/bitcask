package my;

import my.core.Bitcask;
import my.storage.impl.AsyncFlushStorage;
import my.storage.impl.MmapStorage;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args ) throws Exception {
        Bitcask bitcask = Bitcask.builder().storage(new MmapStorage()).build();
//        long start = System.currentTimeMillis();
//        for (int i =0; i < 30000;i++){
//            String key = "key"+i;
//            bitcask.get(key);
//        }
//        System.out.println(System.currentTimeMillis() - start);
        bitcask.put("key1","value1");
        System.out.println(bitcask.get("key1"));
    }
}
