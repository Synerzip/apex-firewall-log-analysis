package com.example.myapexapp.com.example.myapexapp.kafka;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.common.util.BaseOperator;

import java.util.Map;

/**
 * Created by Akshay Harale<akshay.harale@synerzip.com/> on 21/6/16.
 */
public class Aggregator extends BaseOperator{
    public final transient DefaultOutputPort<Map<String, Object>> output = new DefaultOutputPort<Map<String, Object>>();
    public final transient DefaultInputPort<byte[]> input = new DefaultInputPort<byte[]>() {

        @Override
        public void process(byte[] tuple) {


        }
    };
}
