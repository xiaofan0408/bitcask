package my.core;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;

@Data
@AllArgsConstructor
public class Index {

    private String activeFileName;

    private long start;

    private long length;

}
