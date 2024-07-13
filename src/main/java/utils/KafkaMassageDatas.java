package utils;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class KafkaMassageDatas {
//    private String event_id;
//    private String event_type;
//    private String event_level;
//    private String event_name;
//    private String event_time;
//    private String event_target_id;
//    private String user_id_str;
//    private Integer user_id_int;
//    private Integer behavior_id;
//    private String behavior_name;
//    private String event_context;


    private static final String[] eventIds = {"event1", "event2", "event3", "event4", "event5", "event6", "event7", "event8", "event9", "event10"};
    private static final String[] eventTypes = {"CLICK", "VIEW", "COUPON", "SUBMIT"};
    private static final String[] eventLevels = {"NORMAL", "LOW", "MEDIUM", "HIGH"};
    private static final String[] eventNames = {"eventName1", "eventName2", "eventName3", "eventName4", "eventName5"};
    private static final String[] eventTargets = {"eventTarget1", "eventTarget2", "eventTarget3", "eventTarget4"};
    private static final String[] userIdStrs = {"useId1", "useId2", "useId3", "useId4"};
    private static final String[] behaviorNames = {"behavior1", "behavior2", "behavior3", "behavior4"};


    public static String getEventId() {
        double random = Math.random() * 10;
        return eventIds[(int) (random % eventIds.length)];
    }

    public static String getEventType() {
        return eventTypes[(int) (Math.random() * 10 % eventTypes.length)];
    }

    public static String getEventLevel() {
        return eventLevels[(int) (Math.random() * 10 % eventLevels.length)];
    }

    public static String getEventName() {
        return eventNames[(int) (Math.random() * 10 % eventNames.length)];
    }

    public static String getEventTarget() {
        return eventTargets[(int) (Math.random() * 10 % eventTargets.length)];
    }

    public static String userIdStrs() {
        return userIdStrs[(int) (Math.random() * 10 % userIdStrs.length)];
    }

//    public static Integer getUserIdInt() {
//        String string = userIdStrs();
//        return Integer.parseInt(string.substring(string.length() - 1));
//    }

    public static String getBehaviorName() {
        return behaviorNames[(int) (Math.random() * 10 % behaviorNames.length)];
    }

    public static Integer getBehaviorId() {
        String str = getBehaviorName();
        return Integer.parseInt(str.substring(str.length() - 1));
    }

    public static String getEventTime() {
        LocalDateTime now = LocalDateTime.now();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        return now.format(formatter);
    }

    public static void main(String[] args) {
        System.out.println(KafkaMassageDatas.getBehaviorId());
    }
}
