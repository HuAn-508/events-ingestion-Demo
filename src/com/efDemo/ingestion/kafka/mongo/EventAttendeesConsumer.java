package com.efDemo.ingestion.kafka.mongo;

import com.efDemo.ingestion.common.MongoWriter;
import com.efDemo.ingestion.common.Parsable;
import com.efDemo.ingestion.common.Persistable;
import com.efDemo.ingestion.common.Tuple;
import com.efDemo.ingestion.kafka.EFConsumer;
import com.mongodb.BasicDBObject;

public class EventAttendeesConsumer extends EFConsumer {
    //hbase parser
    public static class EventAttendeesMongoParser implements Parsable<Tuple<BasicDBObject,BasicDBObject>> {
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
        public Tuple<BasicDBObject,BasicDBObject> parse(String[] fields) {
            BasicDBObject d = new BasicDBObject();
            d.put("event_id",fields[0]);
            d.put("user_id",fields[1]);
            d.put("attend_type",fields[2]);
            //result
            return new Tuple<>(d,d);
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
        super(new Persistable[] { new MongoWriter("event_attendee",new EventAttendeesMongoParser())});

    }
}