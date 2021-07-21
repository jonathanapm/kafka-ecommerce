package br.com.alura.eccomerce;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Map;

public class EmailService {

    public static void main(String[] args) {
        var emailService = new EmailService();

        try(KafkaService kafkaService = new KafkaService(
                "ECOMMERCE_SEND_EMAIL",
                EmailService.class.getSimpleName(),
                emailService::parse,
                String.class,
                Map.of(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                        StringDeserializer.class.getName()))
        ) {
            kafkaService.run();
        }
    }

    private void parse(ConsumerRecord<String, String> consumer) {
        System.out.println("---------------------------------------------------");
        System.out.println("Send email");
        System.out.println(consumer.key());
        System.out.println(consumer.value());
        System.out.println(consumer.partition());
        System.out.println(consumer.offset());
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("Email sent");
    }


}
