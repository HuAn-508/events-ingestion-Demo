package com.efDemo.ingestion.kafka;

import com.efDemo.ingestion.common.HBaseWriter;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import com.efDemo.ingestion.common.Parsable;
import com.efDemo.ingestion.common.Persistable;

public class EventAttendeesConsumer extends EFConsumer {
    //hbase parser
    public static class EventAttendeesHBaseParser implements Parsable<Put> {
        //check if the record is a header
        public Boolean isHeader(String[] fields) {
            //check
            return (isValid(fields) && fields[0].equals("event_id") && fields[1].equals("user_id") && fields[2].equals("attend_type"));
        }

        //check if a record is valid
        public Boolean isValid(String[] fields) {

            //check - evnet_id, yes, maybe, invited, no

            return (fields.length > 2 && !isEmpty(fields,new int[]{0,1,2}));
        }

        //parse the record
        public Put parse(String[] fields) {
            //create - Row-Key
            //Put p = new Put(Bytes.toBytes((fields[0] + "." + fields[1] + "-" + fields[2]).hashCode()));
            Put p = new Put(Bytes.toBytes((fields[0] + "." + fields[1] + "-" + fields[2])));
            //euat: event_id
            p = p.addColumn(Bytes.toBytes("euat"), Bytes.toBytes("event_id"), Bytes.toBytes(fields[0]));
            //euat: user_id
            p = p.addColumn(Bytes.toBytes("euat"), Bytes.toBytes("user_id"), Bytes.toBytes(fields[1]));
            //euat: attend_typeÅ
            p.addColumn(Bytes.toBytes("euat"), Bytes.toBytes("attend_type"), Bytes.toBytes(fields[2]));
            //the result
            return p;
        }
    }

    //kafka topic
    @Override
    protected String getKafkaTopic() {
        return "event_attendees";
    }

    //the flag for how to commit the consumer reads
    @Override
    protected Boolean getKafkaAutoCommit() {
        return true;
    }

    //the max # of records polled
    @Override
    protected int getMaxPolledRecords() {
        return 32000;
    }

    //consumer group
    @Override
    protected String getKafkaConsumerGrp() {
        return "grpEventAttendees";
    }

    //constructor
    public EventAttendeesConsumer() {
        //call base
        super(new Persistable[]{new HBaseWriter("events_db:event_attendee", new EventAttendeesHBaseParser()) });
    }
}

