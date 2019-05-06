package my;

import static org.junit.Assert.assertTrue;

import my.core.Bitcask;
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
        Bitcask bitcask = new Bitcask();
        bitcask.put("hello","hello world");
        Assert.assertEquals(bitcask.get("hello"),"hello world");
        bitcask.put("hello","hello2");
        bitcask.put("hello1","hello11");
        Assert.assertEquals(bitcask.get("hello"),"hello2");
        Assert.assertEquals(bitcask.get("hello1"),"hello11");
        for (int i =0; i < 100;i++){
            String key = "key"+i;
            String value = "value"+i;
            bitcask.put(key,value);
            Assert.assertEquals(bitcask.get(key),value);
        }
    }


    @Test
    public void testStart() throws Exception {
        Bitcask bitcask = new Bitcask();
        bitcask.get("hello");
    }
}
