package entry;


import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.util.ArrayList;


@Data
@Setter
@Getter
@ToString
public class BodyInfo {
    private String orgId;
    private long timestamp;
    private ArrayList<Info> infos;
}
