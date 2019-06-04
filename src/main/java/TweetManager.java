

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class TweetManager implements LifecycleManager, Serializable {

    private boolean isRunning = true;
    @Override
    public void start() {
        isRunning = true;
        // Criar as propriedades do consumidor
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, TweetDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "consumer_demo");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Criar o consumidor
        KafkaConsumer<String ,Tweet> consumer = new KafkaConsumer<>(properties);

        // Subscrever o consumidor para o nosso(s) tópico(s)
        consumer.subscribe(Collections.singleton("meu_topico"));

        Thread myConsumer = new Thread (new Runnable(){
            public void run(){
                // Ler as mensagens
                while (isRunning) {  // Apenas como demo, usaremos um loop infinito
                    ConsumerRecords<String, Tweet> poll = consumer.poll(Duration.ofMillis(1000));
                    for (ConsumerRecord<String, Tweet> record : poll) {
                        System.out.println(record.topic() + " - " + record.partition() + " - " + record.value());
                    }
                }
            }
        });
        myConsumer.start();
    }

    @Override
    public void stop() {
        isRunning = false;
    }
}
