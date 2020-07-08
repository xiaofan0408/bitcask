package ben;

import my.core.Bitcask;
import my.storage.impl.AsyncFlushStorage;
import my.storage.impl.FileChannelStorage;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

@State(Scope.Benchmark)
public class BitcaskBenchmark {

    @Param({"10000","100000"})
    private Integer length;

    private Bitcask bitcask;

    private byte[] array1024 = new byte[128];

    private  String key = "key";

    private  String value = "value";

    @Setup
    public void init(){
        bitcask = Bitcask.builder()
                .storage(new FileChannelStorage())
                .build();
    }

    @Benchmark
    public void testSet10kb(){
        bitcask.put(key,array1024);
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                // 导入要测试的类
                .include(BitcaskBenchmark.class.getSimpleName())
                // 预热5轮
                .warmupIterations(1)
                // 度量10轮
                .measurementIterations(2)
                .mode(Mode.Throughput)
                .forks(1)
                .output("./log/bitcask3-128.log")
                .build();

        new Runner(opt).run();

    }

}
