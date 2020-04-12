package com.efDemo.ingestion.kafka.streaming;

import com.efDemo.ingestion.IngestionExecutor;
import org.apache.commons.lang.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.ValueMapper;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class EventAttendeesStreamer implements IngestionExecutor {
    public void run() throws Exception {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "EventAttendeesStreamer");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.STATE_DIR_CONFIG, "statestore-ea");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");//or latest

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> data = builder.stream("event_attendees_raw");
        KStream<String, String> resultStream = data.flatMapValues(new ValueMapper<String, Iterable<String>>() {
            @Override
            public Iterable<String> apply(String value) {
                List<String> list = new ArrayList<String>();
                String[] split = value.split(",",-1);
                String event_id = split[0];

                if ( split[1]!= null && split.length > 1 ) {
                    String[] yesUsers = split[1].split(" ");
                    if (yesUsers != null && yesUsers.length > 0){
                        for ( String yesUser : yesUsers) {
                            if (!StringUtils.isBlank(yesUser)) {
                                String newValue = null;
                                newValue = event_id + "," + yesUser + ",yes";
                                list.add(newValue);
                            }
                        }
                    }
                }


                if ( split[2]!= null && split.length > 2 ) {
                    String[] maybeUsers = split[2].split(" ");
                    if (maybeUsers != null && maybeUsers.length > 0){
                        for ( String maybeUser : maybeUsers) {
                            if (!StringUtils.isBlank(maybeUser)) {
                                String newValue = null;
                                newValue = event_id + "," + maybeUser + ",maybe";
                                list.add(newValue);
                            }
                        }
                    }
                }


                if ( split[3]!= null && split.length > 3 ) {
                    String[] invitedUsers = split[3].split(" ");
                    if (invitedUsers != null && invitedUsers.length > 0){
                        for ( String invitedUser : invitedUsers) {
                            if (!StringUtils.isBlank(invitedUser)) {
                                String newValue = null;
                                newValue = event_id + "," + invitedUser + ",invited";
                                list.add(newValue);
                            }
                        }
                    }
                }

                if ( split[4]!= null && split.length > 4) {
                    String[] noUsers = split[4].split(" ");
                    if (noUsers != null && noUsers.length > 0){
                        for ( String noUser : noUsers) {
                            if (!StringUtils.isBlank(noUser)) {
                            String newValue = null;
                            newValue = event_id + "," + noUser + ",no";
                            list.add(newValue);
                        }
                        }
                    }
                }

                return list;

            }
        }).filter((k,v)-> v !=null&&v.length()>0);
        resultStream.to("event_attendees");
        KafkaStreams streams = new KafkaStreams(builder.build(), config);
        streams.start();
    }

    @Override
    public void execute(String[] args) throws Exception {
        EventAttendeesStreamer eventAttendeesStreamer = new EventAttendeesStreamer();
        eventAttendeesStreamer.run();

    }
}
