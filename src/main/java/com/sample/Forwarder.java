package com.sample;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.common.util.BaseOperator;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Created by Akshay Harale<akshay.harale@synerzip.com/> on 23/6/16.
 */
public class Forwarder extends BaseOperator {
    public final transient DefaultOutputPort<Map<String, Object>> output = new DefaultOutputPort<Map<String, Object>>();
    public final transient DefaultInputPort<String> input = new DefaultInputPort<String>() {

        @Override
        public void process(String tuple) {
            if (tuple != null) {
                Map<String, Object> logs = new HashMap<String, Object>();
                String message = null;

                message = tuple;

                logs.put("number",new Integer( message));
                logs.put("uuid", UUID.randomUUID().toString());
                System.out.println(message);
                output.emit(logs);
            }
        }
    };
}
