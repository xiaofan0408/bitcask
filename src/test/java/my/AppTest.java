package my;

import static org.junit.Assert.assertTrue;

import my.core.Bitcask;
import my.storage.impl.AsyncFlushStorage;
import my.storage.impl.MmapStorage;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit test for simple App.
 */
public class AppTest 
{
    /**
     * Rigorous Test :-)
     */
    @Test
    public void shouldAnswerWithTrue()
    {
        assertTrue( true );
    }

    @Test
    public void testGetAndPut() throws Exception {
        Bitcask bitcask = Bitcask.builder().fileChannelStorage().build();
//        bitcask.put("hello","hello world");
//        Assert.assertEquals(bitcask.get("hello"),"hello world");
//        bitcask.put("hello","hello2");
//        bitcask.put("hello1","hello11");
//        Assert.assertEquals(bitcask.get("hello"),"hello2");
//        Assert.assertEquals(bitcask.get("hello1"),"hello11");
        for (int i =0; i < 30000;i++){
            String key = "key"+i;
            String value = "value"+i;
            bitcask.put(key,value);
            Assert.assertEquals(bitcask.get(key),value);
        }
    }

    @Test
    public void testPut() throws Exception {
        Bitcask bitcask = Bitcask.builder().storage(new MmapStorage()).build();
//        bitcask.put("hello","hello world");
//        Assert.assertEquals(bitcask.get("hello"),"hello world");
//        bitcask.put("hello","hello2");
//        bitcask.put("hello1","hello11");
//        Assert.assertEquals(bitcask.get("hello"),"hello2");
//        Assert.assertEquals(bitcask.get("hello1"),"hello11");
        byte[] array128 = new byte[1024];
        String key = "key";
        long start = System.currentTimeMillis();
        for (int i =0; i < 100000;i++){
            bitcask.put(key,array128);
        }
        System.out.println(System.currentTimeMillis() - start);
    }


    @Test
    public void testStart() throws Exception {
        Bitcask bitcask = Bitcask.builder().fileChannelStorage().build();
        for (int i =0; i < 30000;i++){
            String key = "key"+i;
            String value = "value"+i;
            Assert.assertEquals(bitcask.get(key),value);
        }
    }
}
