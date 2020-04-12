package com.efDemo.ingestion.config;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Properties;

public class EFConfig {
    //zoo-keeper url
    public final static String zooKeeperUrl = "zookeeperUrl";
    //broker url
    public final static String kafkaBrokerUrl = "brokerUrl";
    //state directory
    public final static String stateDir = "stateDir";

    //core-site
    public final static String coreSite = "coreSite";
    //hdfs-site
    public final static String hdfsSite = "hdfsSite";
    //hbase-site
    public final static String hbaseSite = "hbaseSite";


    public final static String dbJdbcUrl = "dbJdbcUrl";

    public final static String dbUser = "dbUser";

    public final static String dbPassword = "dbPassword";


//也可用枚举
    public final static String mongoHost = "mongoHost";

    public final static String mongoPort = "mongoPort";

    public final static String mongoDatabase = "mongoDatabase";

    public final static String  mongoUser= " mongoUser";

    public final static String mongoPwd= "mongoPwd";


    //load settings
    public static Properties loadSettings(String settingsFile) throws IOException {
        //the properties
        Properties props = new Properties();
        //open file
        FileInputStream input = new FileInputStream(settingsFile);
        try {
            //reader
            BufferedReader br = new BufferedReader(new InputStreamReader(input));
            try {
                //read first line
                String line = br.readLine();
                //loop for read
                while ( line != null ) {
                    //split
                    String[] kv = line.split("=", -1);
                    //check
                    if ( kv != null && kv.length == 2 ) {
                        //add
                        props.put(kv[0], kv[1]);
                    }
                    //read next
                    line = br.readLine();
                }
            } finally {
                //close
                br.close();
            }
        } finally {
            //close
            input.close();
        }
        //extracts
        return props;
    }
}
