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
    }
}
