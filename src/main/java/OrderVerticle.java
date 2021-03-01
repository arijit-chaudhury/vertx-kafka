import java.util.Properties;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.http.HttpMethod;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.ext.web.handler.ResponseContentTypeHandler;
import io.vertx.kafka.client.producer.KafkaProducer;
import io.vertx.kafka.client.producer.KafkaProducerRecord;
import io.vertx.kafka.client.serialization.JsonObjectSerializer;

public class OrderVerticle extends AbstractVerticle {

    @Override
    public void start(Future<Void> startFuture) throws Exception {
        // Set the properties and create Kafka producer
        Properties config = new Properties();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonObjectSerializer.class);
        config.put(ProducerConfig.ACKS_CONFIG, "1");
        KafkaProducer<String, String> producer = KafkaProducer.create(vertx, config);
        producer.partitionsFor("employees-topic", done -> {
			done.result().forEach(p -> System.out.println("Partition: id={}, topic={}"+": "+p.getPartition()+": "+p.getTopic()));
		});

        Router router = Router.router(vertx);
        router.route("/order/*").handler(ResponseContentTypeHandler.create());
        router.route(HttpMethod.POST, "/order").handler(BodyHandler.create());
        router.post("/order").handler(routingContext -> {
        	System.out.println("Test");
        	KafkaProducerRecord<String,String > record = KafkaProducerRecord.create("employees-topic", null, "From Java Client", 1);
        	producer.write(record);
        	routingContext.response().end("All Ok!");
        });
        vertx.createHttpServer().requestHandler(router::accept).listen(8090);
    }
}
