package org.test;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class VentesReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
    @Override
    protected void reduce(Text key,
                          Iterable<DoubleWritable> values,
                          Reducer<Text, DoubleWritable, Text, DoubleWritable>.Context context) throws IOException, InterruptedException {
        int somme = 0;
        Iterator<DoubleWritable> iterator = values.iterator();

        while (iterator.hasNext()){
           somme += iterator.next().get();
        }

       context.write(key, new DoubleWritable(somme));
    }
}
