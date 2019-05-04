package my;

import my.core.Bitcask;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args ) {

        Bitcask bitcask = new Bitcask();
        bitcask.put("hello","hello world");

    }
}
