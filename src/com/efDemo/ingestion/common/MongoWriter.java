package com.efDemo.ingestion.common;
import com.efDemo.ingestion.config.EFConfig;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.Mongo;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;


import java.util.Properties;

public class MongoWriter implements Persistable{
    private String mongoHost = null;
    private int mongoPort =27017;
    private String mongoDatabase = null;
    private String mongoUser = null;
    private String mongoPassword = null;

    private String mongoCollection;

    private Parsable<Tuple<BasicDBObject,BasicDBObject>> parser;

    public MongoWriter(String mongoCollection,Parsable<Tuple<BasicDBObject,BasicDBObject>> parser){
        this.mongoCollection=mongoCollection;
        this.parser=parser;
    }

    public void initialize(Properties props){
        this.mongoHost = props.getProperty(EFConfig.mongoHost);
        try{
            this.mongoPort = Integer.parseInt(props.getProperty(EFConfig.mongoPort));
        }catch (Exception e){this.mongoPort=27017;}
            this.mongoDatabase = props.getProperty(EFConfig.mongoDatabase);
            this.mongoUser = props.getProperty(EFConfig.mongoUser);
            this.mongoPassword = props.getProperty(EFConfig.mongoPwd);
    }



    public int write(ConsumerRecords<String,String> records){

        int numDocs = 0;
        Mongo mongo = new Mongo(mongoHost,mongoPort);
        try{
            DB db = mongo.getDB(mongoDatabase);
            if (mongoUser !=null && !mongoUser.isEmpty()){
                db.addUser(mongoUser,mongoPassword.toCharArray());
            }
            DBCollection collection = db.getCollection(mongoCollection);
            long passHead = 0;
            for(ConsumerRecord<String,String> record : records){
                try {
                    String[] elements = record.value().split(",",-1);
                    if (passHead == 0 && this.parser.isHeader(elements)){
                        passHead = 1;
                        continue;
                    }

                    if (this.parser.isValid(elements)){
                        //???
                        Tuple<BasicDBObject,BasicDBObject> kv = this.parser.parse(elements);
                        collection.update(kv.key,new BasicDBObject("$set",kv.value),true,false);

                        numDocs++;


                    }else {
                        System.out.println(String.format("ErrorOccured: invalid message found when writing to MongoDB! - %s", record.value()));

                    }
                }catch (Exception e){
                    System.out.println("ErrorOccured" + e.getMessage());
                }
            }
        }finally {
            mongo.close();
        }
        return numDocs;
    }

}
