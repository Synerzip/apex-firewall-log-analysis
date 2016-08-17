package com.example.myapexapp.com.example.myapexapp.kafka;

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
public class KafkaApplication implements StreamingApplication{
    @Override
    public void populateDAG(DAG dag, Configuration conf) {

        KafkaConsumer k = new SimpleKafkaConsumer();
        k.setInitialOffset("earliest");

        KafkaSinglePortStringInputOperator stringInput=dag.addOperator("KafkaReader", new KafkaSinglePortStringInputOperator ());
        k.setTopic("test");

        stringInput.setConsumer(k);
        stringInput.setZookeeper("localhost:2181");

        //KafkaSinglePortInputOperator input = dag.addOperator("KafkaReader", new KafkaSinglePortInputOperator());

        //MessageProcessor processor = dag.addOperator("processor", new MessageProcessor());
        Forwarder forward=dag.addOperator("forward",new Forwarder());

        dag.addStream("message",stringInput.outputPort,forward.input);

        //set up the elastic search output operator

        ElasticSearchMapOutputOperator output= dag.addOperator("fireLogOperator", new ElasticSearchMapOutputOperator ());
        try {
            output.setStore(createStore());
        } catch (IOException e) {
            e.printStackTrace();
        }

        output.setIdField("uuid");
        output.setIndexName("numbers");
        output.setType("number");

        dag.addStream("toElastic",forward.output,output.input);
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
