package entry;

import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Data
@Setter
@Getter
@ToString
public class SinkInfo {
    private long time;
    private double avgValue;
    private double firstValue;
    private double lastValue;
    private double maxValue;
    private double minVlue;
    private String key;
}
