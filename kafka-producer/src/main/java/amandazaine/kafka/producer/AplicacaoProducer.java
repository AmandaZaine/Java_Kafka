package amandazaine.kafka.producer;

import amandazaine.kafka.producer.eventos.ProdutorEvento;

public class AplicacaoProducer {
    public static void main(String[] args) {
        AplicacaoProducer aplicacaoProducer = new AplicacaoProducer();
        aplicacaoProducer.iniciar();
    }

    private void iniciar() {
        ProdutorEvento produtorEvento = new ProdutorEvento();
        produtorEvento.executar();
    }
}
