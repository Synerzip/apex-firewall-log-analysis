package com.sample;

import org.joda.time.DateTime;

/**
 * Created by Akshay Harale<akshay.harale@synerzip.com/> on 23/6/16.
 */
public class Tuple {

    Long rcvdBytes;
    Long sentBytes;
    Long time;

    Tuple(){

    }


    Tuple(Long rcvdBytes, Long sentBytes,Long time) {
        this.rcvdBytes = rcvdBytes;
        this.sentBytes = sentBytes;
        this.time = time;
    }
    Tuple(Long rcvdBytes, Long sentBytes){

    }

    public static Tuple sum(Tuple t1, Tuple t2) {
        Tuple tuple = new Tuple(
                t1.rcvdBytes + t2.rcvdBytes,
                t1.sentBytes + t2.sentBytes,
                new DateTime(t2.time).withTimeAtStartOfDay().getMillis());
        return tuple;
    }
}
