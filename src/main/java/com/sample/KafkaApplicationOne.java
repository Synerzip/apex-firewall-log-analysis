package com.sample;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.contrib.elasticsearch.ElasticSearchConnectable;
import com.datatorrent.contrib.elasticsearch.ElasticSearchMapOutputOperator;
import com.datatorrent.contrib.kafka.KafkaConsumer;
import com.datatorrent.contrib.kafka.KafkaSinglePortStringInputOperator;
import com.datatorrent.contrib.kafka.SimpleKafkaConsumer;
import org.apache.apex.malhar.kafka.KafkaSinglePortInputOperator;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;

/**
 * Created by Akshay Harale<akshay.harale@synerzip.com/> on 16/6/16.
 */
public class KafkaApplicationOne implements StreamingApplication{
    @Override
    public void populateDAG(DAG dag, Configuration conf) {

        KafkaSinglePortInputOperator input = dag.addOperator("KafkaReader", new KafkaSinglePortInputOperator());
        MessageProcessor processor = dag.addOperator("processor", new MessageProcessor());
        dag.addStream("message",input.outputPort,processor.input);
        Aggregator agg = dag.addOperator("agg", new Aggregator());
        dag.addStream("aggregator",processor.output,agg.input);
        dag.setAttribute(agg,Context.DAGContext.STREAMING_WINDOW_SIZE_MILLIS,10000);
        dag.setAttribute(agg, Context.OperatorContext.APPLICATION_WINDOW_COUNT,6);
        //set up the elastic search output operator
        ElasticSearchMapOutputOperator output= dag.addOperator("fireLogOperator", new ElasticSearchMapOutputOperator ());
        try {
            output.setStore(createStore());
        } catch (IOException e) {
            e.printStackTrace();
        }

        output.setIdField("uuid");
        output.setIndexName("agg");
        output.setType("log");

        dag.addStream("toElastic",agg.output,output.input);
    }
    ElasticSearchConnectable createStore() throws IOException {
        ElasticSearchConnectable store = new ElasticSearchConnectable();
        store.setHostName("localhost");
        store.setPort(9300);
        store.setClusterName("elasticsearch");
        store.connect();
        return store;
    }
}
