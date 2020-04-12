package com.efDemo.ingestion.common;

import com.efDemo.ingestion.config.EFConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;

public class MySqlWriter implements Persistable {
    //the jdbc url
    private String jdbcUrl = null;
    //the user name
    private String user = null;
    //the password
    private String password = null;

    //the mysql table
    private String table = null;

    //the parser
    private Parsable<String> parser = null;

    //constructor
    public MySqlWriter(String table, Parsable<String> parser) {
        //set
        this.table = table;
        this.parser = parser;
    }

    //initialize to extract the database configuration
    public void initialize(Properties props) {
        //core-site
        this.jdbcUrl = props.getProperty(EFConfig.dbJdbcUrl);
        //hdfs-site
        this.user = props.getProperty(EFConfig.dbUser);
        //hbase
        this.password = props.getProperty(EFConfig.dbPassword);
    }

    //write
    public int write( ConsumerRecords<String, String> records ) throws Exception {
        //the # of records puts
        int numInserts = 0;
        //check
        if ( this.jdbcUrl == null || this.jdbcUrl.isEmpty() ) {
            //error out
            throw new Exception("The jdbc-url is not initialized.");
        }

        //the connection object
        Connection conn = DriverManager.getConnection(this.jdbcUrl, this.user, this.password);
        try {
            //flags
            long passHead = 0;
            //loop
            for ( ConsumerRecord<String, String> record : records ) {
                //parse event record
                String[] elements = record.value().split(",", -1 );
                //check if the head has been passed
                if ( passHead == 0 && this.parser.isHeader(elements) ) {
                    //flag
                    passHead = 1;
                    //skip
                    continue;
                }

                //parse
                if ( this.parser.isValid(elements) ) {
                    //the statement
                    CallableStatement stmt = conn.prepareCall(this.parser.parse(elements));
                    try {
                        //execute
                        stmt.execute();

                        //increase the counter
                        numInserts++;
                    }
                    finally {
                        //close
                        stmt.close();
                    }
                }
            }
        }
        finally {
            //close the connection
            conn.close();
        }
        return numInserts;
    }
}
