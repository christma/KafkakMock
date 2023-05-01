package entry;

import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Data
@Setter
@Getter
@ToString
public class Info {

    private String maskCode;
    private String tagCode;
    private long value;
    private int time;
    private int quality;

}
