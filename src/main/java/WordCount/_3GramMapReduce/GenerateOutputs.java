package WordCount._3GramMapReduce;

import WordCount.WordCount;
import WordCount.Writable.PairWritableInteger;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class GenerateOutputs {

//
    public static class Map1 extends Mapper<LongWritable, Text, Text, IntWritable> {

        private int counter=0;
        private long corruptedCounter=0;
        public void setup(Context context) {
            System.out.println("Starting to map/reduce");
        }

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String[] splitted = value.toString().split("\t");
            if(splitted.length<3){
                System.out.println("splitted.length<3");
                return;
            }

            String ngram= splitted[0];
            String count = splitted[2];
            IntWritable countWrite=new IntWritable(Integer.valueOf(count));

            String[] ngram_words = ngram.split(" ");

            //check for 3gram with 3rd empty word.
            if(ngram_words.length<3){
                corruptedCounter++;
                return;
            }
            counter++;
            context.write(new Text(ngram_words[0]), countWrite);
            context.write(new Text(ngram_words[1]), countWrite);
            context.write(new Text(ngram_words[2]), countWrite);
            context.write(new Text("sumOfALLWords"), new IntWritable(3 * countWrite.get()));

        }
        public void cleanup(Context c){
            System.out.println("Map1: "+"TotalWritten: "+counter+"corruptedCounter: "+corruptedCounter);
        }
    }

    public static class Map2 extends Mapper<LongWritable, Text, Text, PairWritableInteger> {
        private int counter=0;
        private long corruptedCounter=0;
        public void setup(Context c){
            System.out.println("setup Map2");
        }
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] splitted = value.toString().split("\t");
            if(splitted.length<3){
                System.out.println("splitted.length<3");
                return;
            }
            if(counter<1000){
                System.out.println(value.toString());
            }
            String ngram= splitted[0];
            String count = splitted[2];

            //check for 3gram with 3rd empty word.
            String[] ngram_words = ngram.split(" ");
            if(ngram_words.length<3){
                corruptedCounter++;
                return;
            }
            PairWritableInteger value_Key = new PairWritableInteger(ngram, Integer.valueOf(count));
            counter++;

            context.write(new Text(ngram_words[0] + " " + ngram_words[1]), value_Key);
            context.write(new Text(ngram_words[1] + " " + ngram_words[2]), value_Key);
        }
        public void cleanup(Context c){
            System.out.println("Map2: "+"TotalWritten: "+counter+" corruptedCounter: "+corruptedCounter);
        }
    }

    public static class Reducer2 extends Reducer<Text, PairWritableInteger, Text, IntWritable> {
        Set<String> textSet;//todo: could be waayy to much - look for java heap sapce.
        public void setup(Context context) {
            textSet = new HashSet<String>();
            System.out.println("setup Reduce2");
        }

        public void reduce(Text key, Iterable<PairWritableInteger> values, Context context) throws IOException, InterruptedException {

            int sum = 0;

            for (PairWritableInteger pair : values) {
                int sumOfPairs = pair.getSecond();
                sum += sumOfPairs;
                String gram3 = pair.getFirst();
//                logger.info("value:"+value.get() );
                if (!textSet.contains(gram3)) {
                    //System.out.println("REDUCER2:: added " + gram3 + "to the wonderful set N:" + sum);
                    textSet.add(gram3);
                }
            }
            //this will write all the triplets which are related to the same key- which is a pair
            for (String gram3 : textSet) {
                //note for splitter in the mapper, first we get the key+ 3gram origin.
                context.write(new Text(key.toString() + "T" + gram3), new IntWritable(sum));
            }

            textSet.clear();
            //context.write(key, new IntWritable(sum));
        }
    }


    public static class Map3 extends Mapper<LongWritable, Text, Text, IntWritable> {
        int counter=0;
        int corruptedCounterData=0;
        public void setup(Context c){
            System.out.println("setup Map3");
        }
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] splitted = value.toString().split("\t");
            if(splitted.length<3){
                System.out.println("splitted.length<3");
                corruptedCounterData++;
                return;
            }

            String ngram= splitted[0];
            String count = splitted[2];

            //check for 3gram with 3rd empty word.
            String[] ngram_words = ngram.split(" ");
            if(ngram_words.length<3){
                return;
            }

            context.write(new Text(ngram),new IntWritable(Integer.valueOf(count)) );
        }
        public void cleanup(Context c){
            System.out.println("Map3: "+"TotalWritten: "+counter+"corruptedCounterData: "+corruptedCounterData);
        }
    }


    public static class ReduceSumValue extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void setup(Context c){
            System.out.println("setup Reduce3");
        }
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }
}
