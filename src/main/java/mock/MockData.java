package mock;

import com.alibaba.fastjson2.JSON;
import entry.MockDataUtils;
import entry.OrderInfo;
import org.apache.kafka.clients.producer.Producer;
import utils.ETLUtils;
import utils.KafkaProducerUtils;

import java.util.Random;

//
//private String id;
//private String gender;
//private Integer age;
//private Long price;
//private String os;
public class MockData {
    Producer producer = KafkaProducerUtils.getProducer();

    public void mock() {
//        int id = MockDataUtils.Id();
        int id = MockDataUtils.SkewId();
        String gender = MockDataUtils.gender();
        int age = MockDataUtils.age();
        long price = MockDataUtils.bidPrice();
        String os = MockDataUtils.os();
        long ts = MockDataUtils.getTs();
        OrderInfo orderInfo = new OrderInfo();
        orderInfo.setId(id);
        orderInfo.setOs(os);
        orderInfo.setGender(gender);
        orderInfo.setAge(age);
        orderInfo.setPrice(price);
        orderInfo.setTs(ts);
        String jsonString = JSON.toJSONString(orderInfo);
        System.out.println(jsonString);
        ETLUtils.sendKafka(producer, "mock", jsonString);
    }


    public static void main(String[] args) throws InterruptedException {
        MockData mockData = new MockData();
        Random random = new Random();
        while (true) {
            mockData.mock();
            Thread.sleep(random.nextInt(500));
        }
    }

}
