package amandazaine.kafka.producer.eventos;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.UUID;

public class ProdutorEvento {

    private final Producer<String, String> producer;

    public ProdutorEvento() {
        producer = criarProducer();
    }

    private Producer<String, String> criarProducer() {
        if(producer != null){
            return producer;
        }

        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("serializer.class", "kafka.serializer.DefaultEncoder");

        return new KafkaProducer<>(properties);
    }

    public void executar() {
        String chave = UUID.randomUUID().toString();

        SimpleDateFormat dateFormat = new SimpleDateFormat("dd-MM-yyyy HH:mm:ss.SSS");
        String mensagem = dateFormat.format(new Date()) + " | " + chave + " | NOVA MENSAGEM";

        ProducerRecord<String, String> producerRecord = new ProducerRecord<>("RegistroEvento", chave, mensagem);
        producer.send(producerRecord);
        producer.flush();
        producer.close();
    }
}
