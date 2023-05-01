package entry;


import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Data
@Setter
@Getter
@ToString
public class SomeInfo {

    private String orgId;
    private Long timestamp;
    private String maskCode;
    private String tagCode;
    private long value;
    private int time;
    private int quality;
    private int _year;
    private int _month;
    private int _day;
    private int _hour;
    private int _minute;
    private int _part;
    private String key;

}
