package utils;

import com.alibaba.fastjson2.JSONObject;
import entry.KafkaMassagePO;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import static java.lang.Thread.sleep;

public class KafkaMassageData {


    private static final String[] eventIds = {"event1", "event2", "event3", "event4", "event5", "event6", "event7", "event8", "event9", "event10"};
    private static final String[] eventTypes = {"CLICK", "VIEW", "COUPON", "SUBMIT"};
    private static final String[] eventLevels = {"NORMAL", "LOW", "MEDIUM", "HIGH"};
    private static final String[] eventNames = {"eventName1", "eventName2", "eventName3", "eventName4", "eventName5"};
    private static final String[] eventTargets = {"eventTarget1", "eventTarget2", "eventTarget3", "eventTarget4"};
    private static final String[] userIdStrs = {"useId1", "useId2", "useId3", "useId4"};
    private static final String[] behaviorNames = {"behavior1", "behavior2", "behavior3", "behavior4"};


    private static String getEventId() {
        double random = Math.random() * 10;
        return eventIds[(int) (random % eventIds.length)];
    }

    private static String getEventType() {
        return eventTypes[(int) (Math.random() * 10 % eventTypes.length)];
    }

    private static String getEventLevel() {
        return eventLevels[(int) (Math.random() * 10 % eventLevels.length)];
    }

    private static String getEventName() {
        return eventNames[(int) (Math.random() * 10 % eventNames.length)];
    }

    private static String getEventTarget() {
        return eventTargets[(int) (Math.random() * 10 % eventTargets.length)];
    }

    private static String userIdStrs() {
        return userIdStrs[(int) (Math.random() * 10 % userIdStrs.length)];
    }

    private static Integer getUserIdInt(String userIdStr) {
        return Integer.parseInt(userIdStr.substring(userIdStr.length() - 1));
    }

    private static String getBehaviorName() {
        return behaviorNames[(int) (Math.random() * 10 % behaviorNames.length)];
    }

    private static Integer getBehaviorId(String BehaviorName) {
        return Integer.parseInt(BehaviorName.substring(BehaviorName.length() - 1));
    }

    private static String getEventTime() {
        LocalDateTime now = LocalDateTime.now();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        double random = Math.random();
        if (random < 0.2) {
            return now.minusSeconds((long) (random * 1000)).format(formatter);
        } else {
            return now.format(formatter);
        }
    }

    public static String getJsonMassage() {
        String user_id_str = userIdStrs();
        String behavior_name = getBehaviorName();
        KafkaMassagePO po = new KafkaMassagePO();
        po.setEvent_id(getEventId());
        po.setEvent_type(getEventType());
        po.setEvent_level(getEventLevel());
        po.setEvent_name(getEventName());
        po.setEvent_time(getEventTime());
        po.setEvent_target_id(getEventTarget());
        po.setUser_id_str(user_id_str);
        po.setUser_id_int(getUserIdInt(user_id_str));
        po.setBehavior_id(getBehaviorId(behavior_name));
        po.setBehavior_name(behavior_name);
        po.setEvent_context("");
        return JSONObject.toJSONString(po);
    }

    public static void main(String[] args) {
        for (int i = 0; i < 100; i++) {
            System.out.println(KafkaMassageData.getEventTime());
            try {
                sleep(1000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
