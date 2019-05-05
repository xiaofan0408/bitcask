package my.core;


import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class Index {

    private String activeFileName;

    private long start;

    private long length;

}
