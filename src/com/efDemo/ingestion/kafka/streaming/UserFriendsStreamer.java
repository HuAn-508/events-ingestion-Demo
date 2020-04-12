package com.efDemo.ingestion.kafka.streaming;

import com.efDemo.ingestion.IngestionExecutor;
import org.apache.commons.lang.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class UserFriendsStreamer implements IngestionExecutor {

    public void run() throws Exception {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "UserFriendsStreamer");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.STATE_DIR_CONFIG,"statestore-uf");
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> data = builder.stream("user_friends_raw");
        KStream<String, String> resultStream = data.flatMapValues(new ValueMapper<String, Iterable<String>>() {
            @Override
            public Iterable<String> apply(String value) {
                List<String> list = new ArrayList<String>();

                String[] split = value.split(",",-1);

                String user = split[0];
                //int user_id = Integer.parseInt(user);
                String fields = split[1];

                String[] friends = fields.split(" ",-1);

                if ( friends != null && friends.length > 0) {
                    for ( String friend : friends ) {
                    if (!StringUtils.isBlank(friend)) {
                        String newValue = null;
                        newValue = user + "," + friend;
                        list.add(newValue);
                    }
                    }
                }
                return list;

            }
        }).filter((k,v)-> v !=null&&v.length()>0);

        resultStream.to("user_friends");

        KafkaStreams streams = new KafkaStreams(builder.build(), config);

        streams.start();
    }

    @Override
    public void execute(String[] args) throws Exception {
        UserFriendsStreamer userFriendsStreamer = new UserFriendsStreamer();
        userFriendsStreamer.run();
    }
}