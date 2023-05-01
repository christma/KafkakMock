package entry;


import org.apache.commons.lang3.RandomStringUtils;

import java.util.UUID;

/**
 * orgId
 * time
 * <p>
 * maskCode
 * tagCode
 * value
 * time
 * quality
 */
public class KSMockDataUtils {

    private static String[] orgIdArray = {"570915906037764096", "570915906037764097", "570915906037764098", "570915906037764099"};

    public static String orgId() {
        double random = Math.random() * 17;
        return orgIdArray[(int) (random % orgIdArray.length)];
    }

    public static long timestamp() {
        return System.currentTimeMillis();
    }

    public static String maskCode() {
        return UUID.randomUUID().toString();
    }

    public static String tagCode() {
        return RandomStringUtils.random(10, "abcdefgABCDEFG123456789");
    }


    public static long value() {
        double random = Math.random() * 100000;
        return (long) (random % 100000);
    }

    public static int time() {
        return 0;
    }

    public static int quality() {
        double random = Math.random();
        if (random > 0.1)
            return 192;
        return 0;
    }


    public static void main(String[] args) {
        System.out.println(Math.random());
    }

}
