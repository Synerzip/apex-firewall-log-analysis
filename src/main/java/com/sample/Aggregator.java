package com.sample;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.common.util.BaseOperator;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.joda.time.DateTime;
import scala.Tuple2;

import javax.validation.constraints.Min;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Created by Akshay Harale<akshay.harale@synerzip.com/> on 21/6/16.
 */
public class Aggregator extends BaseOperator {

    public final transient DefaultOutputPort<Map<String, Object>> output = new DefaultOutputPort<Map<String, Object>>();

    protected static final int DEFAULT_BATCH_SIZE = 1000000;
    @Min(1)
    protected int batchSize = DEFAULT_BATCH_SIZE;
    protected transient Queue<FireLog> tupleBatch;

    public final transient DefaultInputPort<FireLog> input = new DefaultInputPort<FireLog>() {

        @Override
        public void process(FireLog tuple) {
            tupleBatch.add(tuple);
            if (tupleBatch.size() >= batchSize) {
                processBatch();
            }
        }
    };

    @Override
    public void endWindow() {
        if (tupleBatch.size() >= 1) {
            processBatch();
        }
    }

    @Override
    public void setup(Context.OperatorContext context) {
        super.setup(context);
        tupleBatch=new ArrayBlockingQueue(DEFAULT_BATCH_SIZE);
    }

    private void processBatch() {
        List<FireLog> fireList= new ArrayList<FireLog>();
        while (!tupleBatch.isEmpty()) {
            FireLog remove = tupleBatch.remove();
            fireList.add(remove);
        }
        Map<String, Object> log = new HashMap<String, Object>();
        Stream<FireLog> fireStream = fireList.stream();
        Map<String, Tuple> collect =
                fireStream
                        .collect(Collectors.groupingBy(
                        FireLog::getBu,
                        Collectors.reducing(
                                new Tuple(0l, 0l, 0l),
                                FireLog::getBytes,
                                Tuple::sum)));
        collect.entrySet()
                .forEach(m -> {
                    Tuple value = m.getValue();
                    Long rcvdBytes = value.rcvdBytes;
                    Long sentBytes = value.sentBytes;
                    Long time = value.time;
                    log.put("bu", m.getKey());
                    log.put("rcvdBytes", rcvdBytes);
                    log.put("sentBytes", sentBytes);
                    log.put("time", time);
                    log.put("uuid", UUID.randomUUID().toString());
                    output.emit(log);
                });
        fireList=null;
    }
}




