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
        ETLUtils.sendKafka(producer, "mock_test", jsonString);
    }

    public static void main(String[] args) throws InterruptedException {
        MockData mockData = new MockData();
        Random random = new Random();
        while (true) {
            mockData.mock();
            Thread.sleep(random.nextInt(1000));
        }
    }

}
