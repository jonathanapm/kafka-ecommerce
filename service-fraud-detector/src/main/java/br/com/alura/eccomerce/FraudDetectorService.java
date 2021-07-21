package br.com.alura.eccomerce;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.HashMap;

public class FraudDetectorService {

    public static void main(String[] args) {

        var fraudService = new FraudDetectorService();

        try(var kafkaService = new KafkaService("ECOMMERCE_NEW_ORDER", FraudDetectorService.class.getSimpleName(),  fraudService::parse, Order.class, new HashMap<>())) {
            kafkaService.run();
        }
    }

    private void parse(ConsumerRecord<String, Order> consumerRecord) {
        System.out.println("---------------------------------------------------");
        System.out.println("Processing new order, checking for fraud");
        System.out.println(consumerRecord.key());
        System.out.println(consumerRecord.value());
        System.out.println(consumerRecord.partition());
        System.out.println(consumerRecord.offset());
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("Order has processed");
    }
}
