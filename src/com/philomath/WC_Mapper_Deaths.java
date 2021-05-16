package com.philomath;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class WC_Mapper_Deaths extends MapReduceBase implements Mapper<LongWritable,Text,Text,IntWritable> {
    //private final static IntWritable one = new IntWritable(1);
    private Text stateTxt = new Text();

    public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output,
                    Reporter reporter) throws IOException {
        if (key.get() == 0){
            return;
        }
        else{
            String line = value.toString();
            if (line.length() > 1){
                String state = line.split(",")[1];
                String deathStr = line.split(",")[2];

                int deathCnt = StringUtils.isNotBlank(deathStr) ? Integer.parseInt(deathStr) : 0;
                //IntWritable deathIntWritable = new IntWritable(deathCnt);

                //stateTxt.set(state);
                output.collect(new Text(state), new IntWritable(deathCnt));
            }
            else{
                return;
            }
        }
    }
}

