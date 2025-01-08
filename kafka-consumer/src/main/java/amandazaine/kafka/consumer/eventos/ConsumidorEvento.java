package amandazaine.kafka.consumer.eventos;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class ConsumidorEvento {

    private final KafkaConsumer<String, String> consumer;

    public ConsumidorEvento() {
        consumer = criarConsumer();
    }

    private KafkaConsumer<String, String> criarConsumer() {
        if(consumer != null) {
            return consumer;
        }

        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("group.id", "default");

        return new KafkaConsumer<>(properties);
    }

    public void executar() {
        List<String> topicos = new ArrayList<>();
        topicos.add("RegistroEvento");

        consumer.subscribe(topicos);

        boolean continuar = true;

        while (continuar) {
            ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord<String, String> record : consumerRecords){
                gravarMensagem(record.topic(), record.partition(), record.value());

                if(record.value().equalsIgnoreCase("fechar")) {
                    continuar = false;
                }
            }
        }

        consumer.close();
    }

    private void gravarMensagem(String topico, int particao, String mensagem) {
        System.out.println("Topico: " + topico);
        System.out.println("Particao: " + particao);
        System.out.println("Mensagem: " + mensagem);
    }
}
