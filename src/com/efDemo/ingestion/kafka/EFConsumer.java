package com.efDemo.ingestion.kafka;

import com.efDemo.ingestion.IngestionExecutor;
import com.efDemo.ingestion.common.Persistable;
import com.efDemo.ingestion.config.EFConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public abstract class EFConsumer implements IngestionExecutor {
    //property - kafka broker url
    private String kafkaBrokerUrl = null;
    //kafka topic
    protected abstract String getKafkaTopic();
    //the flag for how to commit the consumer reads
    protected abstract Boolean getKafkaAutoCommit();
    //the max # of records polled
    protected int getMaxPolledRecords() {
        return 6400;
    }
    //consumer group
    protected abstract String getKafkaConsumerGrp();

    //writers
    private Persistable[] writers = null;

    //constructor
    public EFConsumer(Persistable[] writers) {
        //set
        this.writers = writers;
    }

    //initialize the properties
    public void initialize(Properties props) {
        //load
        this.kafkaBrokerUrl = props.getProperty(EFConfig.kafkaBrokerUrl);
        //check
        if ( this.writers != null && this.writers.length > 0 ) {
            //initialize
            for ( Persistable writer: writers ) {
                //call
                writer.initialize(props);
            }
        }
    }

    //consume
    protected void consume() throws Exception {
        //check
        if ( this.kafkaBrokerUrl == null || this.kafkaBrokerUrl.isEmpty() ) {
            //error out
            throw new Exception("The Kafka broker url is not initialized.");
        }
        //print
        System.out.println("The Kafka BrokerUrl --> " + this.kafkaBrokerUrl);
        //prepare properties for the consumer
        Properties props = new Properties();
        props.put("bootstrap.servers", this.kafkaBrokerUrl);
        props.put("group.id", this.getKafkaConsumerGrp());
        props.put("enable.auto.commit", this.getKafkaAutoCommit() ? "true" : "false");
        props.put("auto.offset.reset", "earliest");
        props.put("request.timeout.ms", "180000");
        props.put("session.timeout.ms", "120000");
        props.put("max.poll.records", Integer.toString(this.getMaxPolledRecords()));
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        //create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
        //the topics

        List<TopicPartition> topics = Arrays.asList(
                new TopicPartition(getKafkaTopic(), 0),
                new TopicPartition(getKafkaTopic(), 1),
                new TopicPartition(getKafkaTopic(), 2)
        );
        //assign
        consumer.assign(topics);
        //consumer.subscribe(Arrays.asList(new String[]{ getKafkaTopic() }));

        //assign(topics)只能消费一个topic和指定分区，subscribe可以多台机器消费多个topic和多个分区
        //start from the beginning 从头消费
       consumer.seek(topics.get(0), 0L);
        //print message
        System.out.println("Consumer subscribed to topic -> " + getKafkaTopic());

        try {
            long msgProcessed = 0L, msgPolled= 0L;
            //loop for reading
            while ( true ) {
                //poll records
                ConsumerRecords<String, String> records = consumer.poll( 1000 );
                //number of records
                int recordsCount = (records != null) ? records.count() : 0;
                //check
                if ( recordsCount <= 0 ) {
                    //sleep for 5 seconds
                    Thread.sleep(3000);
                    //go next
                    continue;
                }
                //print
                System.out.print(String.format("%d messages polled ...", recordsCount));
                msgPolled += recordsCount;

                //check
                if ( this.writers != null && this.writers.length > 0 && recordsCount > 0 ) {
                    //writers
                    for (Persistable writer: writers) {
                        //write
                       msgProcessed += writer.write( records );

                    }

                    //check
                    if ( !this.getKafkaAutoCommit() ) {
                        //commit
                        consumer.commitSync();
                    }
                }
                //print
                System.out.println(String.format("* * * * * %d messages polled.  %d processed! - - - - - - - ",msgPolled,msgProcessed));
            }
        }
        finally {
            //close
            consumer.close();
        }
    }

    //main entry
    public void execute(String[] args) throws Exception {
        //check
        if (args.length < 1) {
            System.out.println(String.format("Usage: %s <settings-file>", this.getClass().getName()));
            System.out.println("<settings-file>: the configuration settings");
            System.out.println("<target>: the target place for persisting event data. It must be either cassandra or hbase");
        }
        else {
            //initialize
            this.initialize(EFConfig.loadSettings(args[0]));
            //consume & persist to hbase
            this.consume();
        }
    }
}
