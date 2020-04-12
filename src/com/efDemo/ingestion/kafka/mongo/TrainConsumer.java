package com.efDemo.ingestion.kafka.mongo;

import com.efDemo.ingestion.common.MongoWriter;
import com.efDemo.ingestion.common.Parsable;
import com.efDemo.ingestion.common.Persistable;
import com.efDemo.ingestion.common.Tuple;
import com.efDemo.ingestion.kafka.EFConsumer;
import com.mongodb.BasicDBObject;

public class TrainConsumer extends EFConsumer {
    //hbase parser
    public static class TrainMongoParser implements Parsable<Tuple<BasicDBObject,BasicDBObject>> {
        //check if the record is a header
        public Boolean isHeader(String[] fields) {
            //check
            return (isValid(fields) && fields[0].equals("user") && fields[1].equals("event") && fields[2].equals("invited")
                    && fields[3].equals("time_stamp") && fields[4].equals("interested") && fields[5].equals("not_interested"));
        }

        //check if a record is valid
        public Boolean isValid(String[] fields) {
            //check
            return (fields.length > 5 );
        }

        //parse the record
        public Tuple<BasicDBObject,BasicDBObject> parse(String[] fields) {
            BasicDBObject k = new BasicDBObject();
            k.put("user",fields[0]);
            k.put("event",fields[1]);

            BasicDBObject d = new BasicDBObject();
            d.put("user",fields[0]);
            d.put("event",fields[1]);
            d.put("invited",fields[2]);
            d.put("time_stamp",fields[3]);
            d.put("interested",fields[4]);
            d.put("not_interested",fields[5]);

            return new Tuple<>(k,d);
        }
    }

    //kafka topic
    @Override
    protected String getKafkaTopic() {
        return "train";
    }
    //the flag for how to commit the consumer reads
    @Override
    protected Boolean getKafkaAutoCommit() {
        return false;
    }
    //consumer group
    @Override
    protected String getKafkaConsumerGrp() {
        return "grpTrain";
    }

    //constructor
    public TrainConsumer() {
        //call base
        super(new Persistable[] { new MongoWriter("train",new TrainMongoParser())});
    }
}
