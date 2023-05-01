package mock;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import entry.Info;
import entry.KSMockDataUtils;
import org.apache.kafka.clients.producer.Producer;
import utils.ETLUtils;
import utils.KSKafkaProducerUtils;

import java.util.Locale;

public class MockKaisi {

    Producer producer = KSKafkaProducerUtils.getProducer();

    public void mock() {
        String orgId = KSMockDataUtils.orgId();
        long timestamp = KSMockDataUtils.timestamp();
        JSONArray vars = new JSONArray();
        for (int i = 0; i < 10; i++) {
            Info info = new Info();
            info.setMaskCode(KSMockDataUtils.maskCode().toUpperCase(Locale.ROOT));
            info.setTagCode(KSMockDataUtils.tagCode());
            info.setValue(KSMockDataUtils.value());
            info.setTime(KSMockDataUtils.time());
            info.setQuality(KSMockDataUtils.quality());
            Object json = JSONObject.toJSON(info);
            vars.add(json);
        }
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("orgId", orgId);
        jsonObject.put("time", timestamp);
        jsonObject.put("vars", vars);

        JSONObject body = new JSONObject();
        body.put("body", jsonObject);
        String value = JSON.toJSONString(body);
        System.out.println(value);
        ETLUtils.sendKafka(producer, "mock", value);
    }

    public static void main(String[] args) throws InterruptedException {
        MockKaisi mockData = new MockKaisi();
        while (true) {
            mockData.mock();
            Thread.sleep(5000);
        }
    }
}
