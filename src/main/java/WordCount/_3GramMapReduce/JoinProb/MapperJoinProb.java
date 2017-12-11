package WordCount._3GramMapReduce.JoinProb;

import WordCount.Writable.PairWritableInteger;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import java.io.IOException;

public class MapperJoinProb {
    public static class MapperJoinProbPair extends org.apache.hadoop.mapreduce.Mapper<Text, IntWritable, Text, PairWritableInteger> {
        private Logger logger = org.apache.log4j.Logger.getLogger(MapperJoinProbPair.class);

        public void map(Text key, IntWritable value, Context context) throws IOException, InterruptedException {


            String[] splitted;
            PairWritableInteger pair = new PairWritableInteger();
            pair.setSecond(value.get());

            splitted = key.toString().split("T");
            String pairWords = splitted[0];
            String tripletWords = splitted[1];
            //Danny Went ,Danny went home 30
            // pair      , triplet    value:globalPairCount
            //this is the pair +3gram
            pair.setFirst(pairWords);
            context.write(new Text(tripletWords), pair);



        }
    }
    public static class MapperJoinProbTriplet extends org.apache.hadoop.mapreduce.Mapper<Text, IntWritable, Text, PairWritableInteger> {

        public void map(Text key, IntWritable value, Context context) throws IOException, InterruptedException {

            PairWritableInteger pair = new PairWritableInteger();
            pair.setSecond(value.get());
            context.write(key, pair);

        }
    }
}
