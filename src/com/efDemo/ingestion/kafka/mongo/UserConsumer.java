package com.efDemo.ingestion.kafka.mongo;

import com.efDemo.ingestion.common.MongoWriter;
import com.efDemo.ingestion.common.Parsable;
import com.efDemo.ingestion.common.Persistable;
import com.efDemo.ingestion.common.Tuple;
import com.efDemo.ingestion.kafka.EFConsumer;

import com.mongodb.BasicDBObject;

public class UserConsumer extends EFConsumer {

    public static class UserMongoParser implements Parsable<Tuple<BasicDBObject,BasicDBObject>> {

        public Boolean isHeader(String[] fields) {
            //check
            return (isValid(fields) && fields[0].equals("user_id") && fields[1].equals("locale") && fields[2].equals("birthyear")
                    && fields[3].equals("gender") && fields[4].equals("joinedAt") && fields[5].equals("location") && fields[6].equals("timezone"));
        }

        //check if a record is valid
        public Boolean isValid(String[] fields) {
            //check
            return (fields.length > 6 );
        }

        //parse the record
        public Tuple<BasicDBObject,BasicDBObject> parse(String[] fields) {
           BasicDBObject k = new BasicDBObject();
           k.put("user_id",fields[0]);

           BasicDBObject d = new BasicDBObject();
           d.put("user_id",fields[0]);

           BasicDBObject profile = new BasicDBObject();
           profile.put("birth_year",fields[2]);
           profile.put("gender",fields[3]);
           d.put("profile",profile);


           BasicDBObject region = new BasicDBObject();
           region.put("locale",fields[1]);
           region.put("location",fields[5]);
           region.put("time_zone",fields[6]);
           d.put("region",region);

           BasicDBObject registration = new BasicDBObject();
            registration.put("joined_at",fields[4]);

           d.put("registration",registration);
            //result
            return new Tuple<>(k,d);
        }
    }

    //kafka topic
    @Override
    protected String getKafkaTopic() {
        return "users";
    }
    //the flag for how to commit the consumer reads
    @Override
    protected Boolean getKafkaAutoCommit() {
        return false;
    }
    //consumer group
    @Override
    protected String getKafkaConsumerGrp() {
        return "grpUsers";
    }

    //constructor
    public UserConsumer() {
        super(new Persistable[] { new MongoWriter("users",new UserMongoParser())});
        //call base
       // super(new Persistable[] { new HBaseWriter("events_db:users", new UserConsumer.UserHBaseParser()) });
    }
}
