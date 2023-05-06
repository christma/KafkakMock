package entry;

import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Data
@Setter
@Getter
@ToString
public class OrderInfo {
    private Integer id;
    private String gender;
    private Integer age;
    private Long price;
    private String os;
    private Long ts;

}
