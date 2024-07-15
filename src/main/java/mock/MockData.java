package mock;


import org.apache.kafka.clients.producer.Producer;
import utils.ETLUtils;
import utils.KafkaMassageData;
import utils.KafkaProducerUtils;

import java.util.Random;

public class MockData {
    Producer producer = KafkaProducerUtils.getProducer();

    public void mock() {
        String jsonString = KafkaMassageData.getJsonMassage();
        System.out.println(jsonString);
        ETLUtils.sendKafka(producer, "mock-risk", jsonString);
    }

    public static void main(String[] args) throws InterruptedException {
        MockData mockData = new MockData();
        Random random = new Random();
        boolean flag = true;
        int i = 0;
        while (flag) {
            mockData.mock();
            i += 1;
            Thread.sleep(random.nextInt(5000));
            if (i == 99) {
                flag = false;
            }
        }
    }

}
