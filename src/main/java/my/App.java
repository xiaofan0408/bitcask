package my;

import my.core.Bitcask;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args ) throws Exception {
        Bitcask bitcask = new Bitcask();
        long start = System.currentTimeMillis();
        for (int i =0; i < 30000;i++){
            String key = "key"+i;
            bitcask.get(key);
        }
        System.out.println(System.currentTimeMillis() - start);
    }
}
