package com.efDemo.ingestion.kafka.mongo;


import com.efDemo.ingestion.common.MongoWriter;
import com.efDemo.ingestion.common.Parsable;
import com.efDemo.ingestion.common.Persistable;
import com.efDemo.ingestion.kafka.EFConsumer;
import com.efDemo.ingestion.common.Tuple;
import com.mongodb.BasicDBObject;

public class EventConsumer extends EFConsumer {
    //hbase parser
    public static class EventMongoParser implements Parsable<Tuple<BasicDBObject,BasicDBObject>> {
        //check if the record is a header
        public Boolean isHeader(String[] fields) {
            //check
            return (isValid(fields) && fields[0].equals("event_id") && fields[1].equals("user_id") && fields[2].equals("start_time")
                    && fields[3].equals("city") && fields[4].equals("state") && fields[5].equals("zip") && fields[6].equals("country")
                    && fields[7].equals("lat") && fields[8].equals("lng"));
        }

        //check if a record is valid
        public Boolean isValid(String[] fields) {
            //check
            return (fields.length > 8 );
        }

        public Tuple<BasicDBObject,BasicDBObject> parse(String[] fields) {
            BasicDBObject k = new BasicDBObject();
            k.put("event_id",fields[0]);

            BasicDBObject d = new BasicDBObject();
            d.put("event_id",fields[0]);

            BasicDBObject schedule = new BasicDBObject();
            schedule.put("start_time",fields[2]);
            d.put("schedule",schedule);


            BasicDBObject location = new BasicDBObject();
            location.put("city",fields[3]);
            location.put("state",fields[4]);
            location.put("zip",fields[5]);

            location.put("country",fields[6]);
            location.put("lat",fields[7]);
            location.put("lng",fields[8]);

            d.put("location",location);

            BasicDBObject creator = new BasicDBObject();
            creator.put("user_id",fields[1]);
            d.put("creator",creator);

            StringBuffer sbf = new StringBuffer();
            if ( fields.length > 8 ) {
                //check
                for ( int i = 9; i < fields.length; i++ ) {
                    //check
                    if ( sbf.length() > 0 )
                        sbf.append( "|" );
                    //append
                    sbf.append( fields[i] );
                }
            }
            //remark
            BasicDBObject remark = new BasicDBObject();
            remark.put("common_words",sbf.toString());
            d.put("remark",remark);


            //result
            return new Tuple<>(k,d);
        }
    }

    //kafka topic
    @Override
    protected String getKafkaTopic() {
        return "events";
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
        return "grpEvents";
    }

    //constructor
    public EventConsumer() {
        //call base
        super(new Persistable[] { new MongoWriter("events",new EventMongoParser())});

    }
}
