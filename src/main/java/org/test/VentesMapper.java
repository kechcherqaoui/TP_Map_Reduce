package org.test;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class VentesMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
    @Override
    protected void map(LongWritable key,
                       Text value,
                       Mapper<LongWritable, Text, Text, DoubleWritable>.Context context) throws IOException, InterruptedException {
       String vente[]= value.toString().split(" ");
       String ville = vente[1];
       String [] date = vente[0].split("-");
       String year = date[date.length - 1];
       double price = Double.parseDouble(vente[3]);

       context.write(new Text(ville + "-" + year), new DoubleWritable(price));
    }
}
