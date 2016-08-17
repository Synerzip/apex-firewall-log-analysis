/**
 * Put your copyright and license info here.
 */
package com.example.myapexapp;

import com.datatorrent.api.LocalMode;
import com.sample.KafkaApplicationOne;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

/**
 * Test the DAG declaration in local mode.
 */
public class ApplicationTest {

    @Test
    public void testKafkaApplication() throws Exception {

        LocalMode lma = LocalMode.newInstance();
        Configuration conf = new Configuration(false);
        conf.addResource(this.getClass().getResourceAsStream("/META-INF/properties.xml"));
        lma.prepareDAG(new KafkaApplicationOne(), conf);
        LocalMode.Controller lc = lma.getController();

        lc.run(120000);
    }

}
