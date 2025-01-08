package amandazaine.kafka.consumer;

import amandazaine.kafka.consumer.eventos.ConsumidorEvento;

public class AplicacaoConsumer {
    public static void main(String[] args) {
        AplicacaoConsumer aplicacaoConsumer = new AplicacaoConsumer();
        aplicacaoConsumer.iniciar();
    }

    private void iniciar() {
        ConsumidorEvento consumidorEvento = new ConsumidorEvento();
        consumidorEvento.executar();
    }
}
