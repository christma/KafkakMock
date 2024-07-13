package entry;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
public class KafkaMassagePO {
    private String event_id;
    private String event_type;
    private String event_level;
    private String event_name;
    private String event_time;
    private String event_target_id;
    private String user_id_str;
    private Integer user_id_int;
    private Integer behavior_id;
    private String behavior_name;
    private String event_context;

    public KafkaMassagePO(String event_id, String event_type, String event_level, String event_name, String event_time, String event_target_id, String user_id_str, Integer user_id_int, Integer behavior_id, String behavior_name, String event_context) {
        this.event_id = event_id;
        this.event_type = event_type;
        this.event_level = event_level;
        this.event_name = event_name;
        this.event_time = event_time;
        this.event_target_id = event_target_id;
        this.user_id_str = user_id_str;
        this.user_id_int = user_id_int;
        this.behavior_id = behavior_id;
        this.behavior_name = behavior_name;
        this.event_context = event_context;
    }
}
