package WordCount._3GramMapReduce;

import WordCount.Writable.PairWritableDouble;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;


public class SortProb {
    public static class MapSortProb extends Mapper<Text, DoubleWritable, Text, PairWritableDouble> {

        public void map(Text key, DoubleWritable value, Context context) throws IOException, InterruptedException {

            String[] splitted=key.toString().split(" ");
            PairWritableDouble pair=new PairWritableDouble(splitted[2],value.get());

            String w1w2=splitted[0]+" "+splitted[1];
            context.write(new Text(w1w2), pair);
        }
    }


    public static class ReduceSortProb extends Reducer<Text, PairWritableDouble, Text, DoubleWritable> {
        public void reduce(Text key, Iterable<PairWritableDouble> values, Context context) throws IOException, InterruptedException {
            List<PairWritableDouble> valueList=new ArrayList<PairWritableDouble>();

            //wordaround for now: add this to a set.
            Set<PairWritableDouble> set=new HashSet();
            System.out.println("In reduce");
            for (PairWritableDouble value : values) {
                System.out.println("reducer:"+value.getFirst());
                if(!set.add(value)){
                    System.out.println("reducer:"+value.getFirst() +"|already in set.");
                }
            }
            valueList.addAll(set);

            System.out.println("finish adding in reduce");

            Collections.sort(valueList );
            for(PairWritableDouble pair:valueList){
                context.write(new Text(key.toString() +" "+pair.getFirst()),new DoubleWritable(pair.getSecond()) );
            }

        }
    }
}


