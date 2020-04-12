package com.efDemo.ingestion.kafka.mongo;

import com.efDemo.ingestion.common.MongoWriter;
import com.efDemo.ingestion.common.Parsable;
import com.efDemo.ingestion.common.Persistable;
import com.efDemo.ingestion.common.Tuple;
import com.efDemo.ingestion.kafka.EFConsumer;
import com.mongodb.BasicDBObject;

public class UserFriendsConsumer extends EFConsumer {
    //hbase parser
    public static class UserFriendsMongoParser implements Parsable<Tuple<BasicDBObject,BasicDBObject>> {
        //check if the record is a header
        public Boolean isHeader(String[] fields) {
            //check
            return (isValid(fields) && fields[0].equals("user_id") && fields[1].equals("friend_id"));
        }

        //check if a record is valid
        public Boolean isValid(String[] fields) {
            //check
            return (fields.length > 1 && !isEmpty(fields,new int[]{0,1}));
        }

        public Tuple<BasicDBObject,BasicDBObject> parse(String[] fields) {

            BasicDBObject d = new BasicDBObject();
            d.put("user_id",fields[0]);
            d.put("friend_id",fields[1]);
            return new Tuple<>(d,d);

        }
    }

    //kafka topic
    @Override
    protected String getKafkaTopic() {
        return "user_friends";
    }
    //the flag for how to commit the consumer reads
    @Override
    protected Boolean getKafkaAutoCommit() {
        return false;
    }
    //the max # of records polled
    @Override
    protected int getMaxPolledRecords() {
        return 32000;
    }
    //consumer group
    @Override
    protected String getKafkaConsumerGrp() {
        return "grpUserFriends";
    }

    //constructor
    public UserFriendsConsumer() {

        super(new Persistable[] { new MongoWriter("user_friend",new UserFriendsMongoParser())});
    }
}

