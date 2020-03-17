package org.traccar.handler;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.traccar.BaseDataHandler;
import org.traccar.config.Config;
import org.traccar.model.Position;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.netty.channel.ChannelHandlerContext;

public class KafkaTopicHandler extends BaseDataHandler {

	Producer<String, String> kafkaProducer;
	private static final TestCallback CALLBACK = new TestCallback();
	private static final ObjectMapper MAPPER = new ObjectMapper();

	public KafkaTopicHandler(Config config) {

	}

	@Override
	public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.put(ProducerConfig.ACKS_CONFIG, "all");
		props.put(ProducerConfig.RETRIES_CONFIG, 0);
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

		kafkaProducer = new KafkaProducer<String, String>(props);
	}

	@Override
	public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
		if (kafkaProducer != null) {
			kafkaProducer.close();
		}
		super.handlerRemoved(ctx);
	}

	@Override
	protected Position handlePosition(Position position) {
		if (position != null) {
			try {
				ProducerRecord<String, String> data = new ProducerRecord<String, String>("platform-ingestion", Long.toString(position.getId()), MAPPER.writeValueAsString(position));
				kafkaProducer.send(data, CALLBACK);
			} catch (JsonProcessingException e) {
				e.printStackTrace();
			}
		}
		return position;
	}

	private static class TestCallback implements Callback {
		@Override
		public void onCompletion(RecordMetadata recordMetadata, Exception e) {
			if (e != null) {
				System.out.println("Error while producing message to topic :" + recordMetadata);
				e.printStackTrace();
			} else {
				String message = String.format("sent message to topic:%s partition:%s  offset:%s",
						recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset());
				System.out.println(message);
			}
		}
	}

}
