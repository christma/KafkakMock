package entry;

import org.apache.commons.lang3.RandomStringUtils;

import java.util.Random;

public class MockDataUtils {
    static String[] provinceArray = {"北京", "上海", "重庆", "天津", "香港", "澳门", "台湾"};

    public static long getTs() {
        long currentTimeMillis = System.currentTimeMillis();
        return currentTimeMillis;
    }

    public static String gender() {
        double random = Math.random();
        if (random > 0.4)
            return "男";
        else
            return "女";
    }

    public static int age() {
        double random = Math.random() * 70;
        return (int) (random % 100) + 18;
    }

    public static int Id() {
        Random random = new Random();
        return random.nextInt(10);
    }

    public static String country() {
        double random = Math.random();
        if (random > 0.13)
            return "中国";
        else
            return "其他";
    }

    public static String province() {
        double random = Math.random() * 17;
        return provinceArray[(int) (random % provinceArray.length)];
    }

    public static long bidPrice() {
        double random = Math.random() * 100000;
        return (long) (random % 100000);
    }

    //    String requestId, String deviceId, String os, String network, String userId, String sourceType,
//    String bidType, long posId, long accountId, long unitId, long creativeId,
    public static String requestId() {
        return RandomStringUtils.random(15, "abcdefgABCDEFG123456789");
    }

    public static String deviceId() {
        return RandomStringUtils.random(10, "abcdefgABCDEFG123456789");
    }

    public static String os() {
        double random = Math.random();
        if (random > 0.4)
            return "IOS";
        else
            return "ANDROID";
    }

    public static String network() {
        double random = Math.random();
        if (random > 0.4)
            return "4G";
        else if (random > 0.1)
            return "WIFI";
        else
            return "3G";
    }

    public static String userId() {
        return RandomStringUtils.random(8, "abcdefgABCDEFG123456789");
    }

    public static String sourceType() {
        double random = Math.random();
        if (random > 0.3)
            return "DSP";
        else
            return "ADX";
    }

    public static String bidType() {
        double random = Math.random();
        if (random > 0.4)
            return "CPC";
        else if (random > 0.13)
            return "CPM";
        else
            return "CPA";
    }

    public static long posId() {
        double random = Math.random();
        if (random > 0.74)
            return 5;
        else if (random > 0.33)
            return 9;
        else
            return 17;
    }

    public static long accountId() {
        double random = Math.random();
        if (random > 0.54)
            return 88888;
        else if (random > 0.33)
            return 66666;
        else
            return 33333;
    }

    public static long unitId(long accountId) {
        int prefix = new Random(accountId).nextInt(100000);
        return prefix * 10 + new Random().nextInt(5);
    }

    public static long creativeId(long unitId) {
        int prefix = new Random(unitId).nextInt(100000);
        return prefix * 10 + new Random().nextInt(10);
    }
}
