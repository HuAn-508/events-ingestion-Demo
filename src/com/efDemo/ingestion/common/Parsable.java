package com.efDemo.ingestion.common;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.hbase.client.Put;

public interface Parsable<T> {
    //check if the record is a header
    Boolean isHeader (String[] fields);
    //check if a record is valid
    Boolean isValid(String[] fields);
    //parse the record

    default Boolean isEmpty(String[] fields, int[] excludeIndexs){

        Boolean empty = false;

        if(fields != null && fields.length > 0){

            for(int i = 0; i<fields.length ; i++){

                if(excludeIndexs != null && ArrayUtils.contains(excludeIndexs,i)){

                    empty |=(fields[i] == null || fields[i].replaceAll("\\s*", "").length() <= 0 );

                }

            }
        }
        return empty;
    }

    T parse(String[] fields);
}

