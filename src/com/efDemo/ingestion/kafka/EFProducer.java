package com.efDemo.ingestion.kafka;

import com.efDemo.ingestion.IngestionExecutor;
import com.efDemo.ingestion.config.EFConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.Properties;

public class EFProducer implements IngestionExecutor {
    //execute
    public void execute(String[] args) throws Exception {
        //check arguments
        if (args.length != 3) {
            //print usage
            System.out.println("EFProducer settings-file topic file-name");
        }
        else {
            //assign broker url
            String brokerUrl = EFConfig.loadSettings(args[0]).getProperty(EFConfig.kafkaBrokerUrl);
            //print
            System.out.println("The Kafka BrokerUrl --> " + brokerUrl);
            //assign topicName to string variable
            String topicName = args[1].toString();
            //print
            System.out.println("The Kafka Topic --> " + topicName);
            //assign file name
            String fileName = args[2].toString();
            //print
            System.out.println("The Input File --> " + fileName);

            //create an instance for properties to access the producer configs
            Properties props = new Properties();
            //assign localhost id
            props.put("bootstrap.servers", brokerUrl);
            //set acknowlegements for producer requests
            props.put("acks", "all");
            //if the request fails, the producer can automatically retry
            props.put("retries", 0);
            //specify buffer size in config
            props.put("batch.size", 16384);
            //reduce the # of requests less than 0
            props.put("linger.ms", 1);
            //the buffer.memory controls the total amount of memory available to the producer
            props.put("buffer.memory", 33554432);
            //key serializer
            props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            //value serializer
            props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

            //create producer
            Producer<String, String> producer = new KafkaProducer<String, String>(props);
            try {
                //file
                BufferedReader br = new BufferedReader( new FileReader(new File(fileName)) );
                try {
                    //osition
                    long pos = 0, count = 0;
                    //text
                    String text = br.readLine();
                    //check
                    while ( text != null ) {
                        //adjust position
                        pos += text.length() + 2;
                        //send
                        producer.send( new ProducerRecord<String, String>(topicName, Long.toString(pos), text) );
                        //count
                        count++;
                        //next read
                        text = br.readLine();
                    }

                    //print
                    System.out.println( Long.toString(count) + " messages sent." );
                }
                finally {
                    //close the reader
                    br.close();
                }
            }
            catch ( java.lang.Exception e ) {
                //print
                e.printStackTrace();
            }
            finally {
                //close
                producer.close();
            }
        }
    }
}
