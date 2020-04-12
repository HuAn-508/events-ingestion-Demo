package com.efDemo.ingestion.common;

import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.util.Properties;

public interface Persistable {
    //initialize
    void initialize(Properties props);

    //write
    int write( ConsumerRecords<String, String> records ) throws Exception;
}
