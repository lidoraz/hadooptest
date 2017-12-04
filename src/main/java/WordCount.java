
import java.io.*;
import java.net.URI;
import java.rmi.UnexpectedException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.logging.FileHandler;

import N_Gram.NgramInputFormat;
import N_Gram.TripletsInputFormat;
import com.sun.corba.se.spi.ior.Writeable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileAsTextInputFormat;
import org.apache.hadoop.mapreduce.lib.join.TupleWritable;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.jboss.netty.util.internal.ConcurrentHashMap;


import org.apache.log4j.Logger;

public class WordCount {

    //todo:change this
    final static String userPath = "/user/Administrator";

    public static void main(String[] args) throws Exception {
//        String inputFile="/dataHeb";
//        String output1="/output1";
//        String output2="/output2";
//        String output3="/output3";

        //files: all tuples occurensces.
        //      all pairs occurenets  w1 w2 w3 - > w1 w2 , w2 w3
        //      all single occurenes.
        //      add special word where "word" is the sum of all words.
        //j.setInputFormatClass(LzoTextInputFormat.class); //added to support n-grams dataset.
        /**
         * output1 - singles
         * output2 - pairs todo: with triplets related
         * output3 - triplets
         */


        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        //TODO: צריך לראות משהו עם האינפוט, זה לא דוחף ישירות את האגרומנים לתוך הפאת'

        //go over all the corpus, generate triples,pairs,and single table with all occurrences.
        boolean isBuildDataSet = false;
        if (files.length > 2) {
            isBuildDataSet = files[2].contains("build");
        }


        if (isBuildDataSet) {
            boolean isSuccess = initData(c, files[0], files[1]);
            if (!isSuccess) {
                System.exit(1);
            }
        }


        String tripletsInput = "/output3/part-r-00000";

        Job j3 = new Job(c, "last JOB");
        j3.setJarByClass(WordCount.class);
        j3.setMapperClass(Mapper1.class);
        j3.setReducerClass(Reducer1.class);
        j3.setOutputKeyClass(Text.class);
        j3.setOutputValueClass(IntWritable.class);
        j3.setInputFormatClass(TripletsInputFormat.class);
        TripletsInputFormat.addInputPath(j3, new Path(tripletsInput));
        FileOutputFormat.setOutputPath(j3, new Path(files[1]));

//        Job job = Job.getInstance(new Configuration());
//        job.addCacheFile(new URI ("/path/to/file.csv"));

        System.exit(j3.waitForCompletion(true) ? 0 : 1);
    }

    public static boolean initData(Configuration c, String in, String output) throws IOException, ClassNotFoundException, InterruptedException {

        Path input = new Path(in);
        Path output1 = new Path(output + "1");
        Path output2 = new Path(output + "2");
        Path output3 = new Path(output + "3");

        Job j1 = new Job(c, "job1");
        j1.setJarByClass(WordCount.class);
        j1.setMapperClass(Map1.class);
        j1.setReducerClass(ReduceSumValue.class);
        j1.setOutputKeyClass(Text.class);
        j1.setOutputValueClass(IntWritable.class);
        j1.setInputFormatClass(NgramInputFormat.class);
        //NgramInputFormat
        NgramInputFormat.addInputPath(j1, input);

        FileOutputFormat.setOutputPath(j1, output1);

        Job j2 = new Job(c, "job2");
        j2.setJarByClass(WordCount.class);
        j2.setMapperClass(Map2.class);
        j2.setReducerClass(Reducer2.class);
        j2.setOutputKeyClass(Text.class);
        j2.setOutputValueClass(IntWritable.class);
        j2.setInputFormatClass(NgramInputFormat.class);
        NgramInputFormat.addInputPath(j2, input);
        FileOutputFormat.setOutputPath(j2, output2);

        Job j3 = new Job(c, "job3");
        j3.setJarByClass(WordCount.class);
        j3.setMapperClass(Map3.class);
        j3.setReducerClass(ReduceSumValue.class);
        j3.setOutputKeyClass(Text.class);
        j3.setOutputValueClass(IntWritable.class);
        j3.setInputFormatClass(NgramInputFormat.class);
        NgramInputFormat.addInputPath(j3, input);
        FileOutputFormat.setOutputPath(j3, output3);

        boolean status1 = j1.waitForCompletion(true);
        boolean status2 = j2.waitForCompletion(true);
        boolean status3 = j3.waitForCompletion(true);

        return (status1 && status2 && status3);

    }

    public static class Mapper1 extends Mapper<Text, IntWritable, Text, TupleWritable> {
        private Logger logger = org.apache.log4j.Logger.getLogger(Mapper1.class);


        //todo: http://dailyhadoopsoup.blogspot.co.il/2014/01/mutiple-input-files-in-mapreduce-easy.html

        public void map(Text key, IntWritable value, Context context) throws IOException, InterruptedException {
            String[] splitted = key.toString().split(" ");

            Writable[] values = new Writable[2];
            //todo: change values.length==3, it will hold
            values[1] = value;
            if (splitted.length == 5) {

                //Danny Went ,Danny went home 30, 5
                // pair      , triplet    globalPairCount, triplet count
                //todo: debug here
                //todo: Change intWritable into Long Writable
                //this is the pair +3gram
                String gram3 = splitted[2] + " " + splitted[3] + " " + splitted[4];
                values[0] = new Text(splitted[0] + " " + splitted[1]);

                context.write(new Text(gram3), new TupleWritable(values));
            } else {
                values[0] = key;

                context.write(key, new TupleWritable(values));
            }

        }
    }

    public static class Reducer1 extends Reducer<Text, TupleWritable, Text, DoubleWritable> {

        //private static ConcurrentHashMap<String,Integer> pairsMap=new ConcurrentHashMap<String, Integer>();
        private static ConcurrentHashMap<String, Integer> singlesMap = new ConcurrentHashMap<String, Integer>();

        private static StringBuilder output;

        private Logger logger = org.apache.log4j.Logger.getLogger(Reducer1.class);
        private int expCounter = 0;
        private String currentTriplete = null;
        private String currentPairs = null;


        public void setup(Reducer.Context context) throws IOException {

            output = new StringBuilder();
            //todo: add make the hashmap sharable among other reducers, so it wont be created over and over
            singlesMap = new ConcurrentHashMap<String, Integer>();
            //load data
            FileSystem fs = FileSystem.get(context.getConfiguration());
            FSDataInputStream singlesInputStream = fs.open(new Path("/output1/part-r-00000")); //singles

            BufferedReader reader = new BufferedReader(new InputStreamReader(singlesInputStream, "UTF-8"));
            String line;
            String[] splited;
//                output.append("@@going on singles\n");
            while ((line = reader.readLine()) != null) {
                splited = line.split("\t");
                singlesMap.put(splited[0], Integer.valueOf(splited[1]));
            }
            reader.close();
            logger.info("-reducer1: finish setup");

        }

        public void reduce(Text key, Iterable<TupleWritable> values, Context context) throws IOException, InterruptedException {
            String w1;
            String w2;
            String w3;
            int N1;
            int N2 = 0;
            int N3 = 0;
            int C0;
            int C1;
            int C2 = 0;
            String[] splited=key.toString().split(" ");
            long relvantPairCount1=0,relvantPairCount2=0;
            //got 3gram key. //all values here are related to the same 3gram
            //todo: how do we get the 3gram word count? maybe add it as a value to the output2
            // so a key would be
            // Danny went home , valOfDanny
            // Danny went home , Danny went, dannyWentCount
            // Danny went home , went home, wentHomeCount.

            if (splited.length == 3) {
                for(TupleWritable tuple:values){
                    Text relvantPair=(Text)tuple.get(0);

                    long relvantPairCount=((LongWritable)tuple.get(1)).get();

                    //check if its the first pair or the second
                    String relvantPairString=relvantPair.toString();
                    if(relvantPairString.equals(splited[0]+" "+splited[1])){
                        relvantPairCount1+=relvantPairCount;
                    }
                    else if(relvantPairString.equals(splited[1]+" "+splited[2])){
                        relvantPairCount2+=relvantPairCount;
                    }
                    else{
                        throw new IllegalStateException("Wrong Comparision in reduce");
                    }


                }

            }
            //got pair
            else{
                //todo: should we ignore it?
            }
//
//            String gram3 = splitted[2] + " " + splitted[3] + " " + splitted[4];
//            values[0] = new Text(splitted[0] + " " + splitted[1]);
//            context.write(new Text(gram3), new TupleWritable(values));

//            for (TupleWritable value : values) {
//                //triplets
//                if (((IntWritable) value.get(1)).get() == -1) {
//                    currentTriplete = key.get(0).toString() + " " + key.get(1).toString();
//                    N3 = ((IntWritable) value.get(0)).get();
//                }
//                //pairs
//                else {
//                    currentPairs = key.get(0).toString() + " " + key.get(1).toString();
//                    C2 = ((IntWritable) value.get(0)).get();
//                    N2 = ((IntWritable) value.get(1)).get();
//                }
//            }

//            if (currentTriplete.equals(currentPairs)) {
                splited = key.toString().split(" ");
                w1 = splited[0];
                w2 = splited[1];
                w3 = splited[3];
                N1 = singlesMap.get(w3);
                C1 = singlesMap.get(w2);
                C0 = singlesMap.get("sumOfALLWords");
                double k2 = (Math.log10(N2 + 1) + 1) / (Math.log10(N2 + 1) + 2);
                double k3 = (Math.log10(N3 + 1) + 1) / (Math.log10(N3 + 1) + 2);
                double prob = k3 * (N3 / C2) + (1 - k3) * k2 * (N2 / C1) + (1 - k3) * (1 - k2) * (N1 / C0);
                context.write(new Text(w1 + w2 + w3), new DoubleWritable(prob));
//            }

//            String[] splited = key.toString().split(" ");
//
//            try{
//                String w1 = splited[0];
//                String w2 = splited[1];
//                String w3 = splited[2];
//                Integer n1=singlesMap.get(w3);
//                Integer n2 = pairsMap.get(w2 + " " + w3);
//                int n3 = sum;
//                Integer c0 = singlesMap.get("sumOfALLWords");
//                Integer c1 = singlesMap.get(w2);
//                Integer c2 = pairsMap.get(w1 + " " + w2);
//                if(n1==null){
//                    logger.warn("got null for "+ w3);
//                    return;
//                }
//                if(n2==null){
//                    logger.warn("got null for "+ w2 + " " + w3);
//                    return;
//                }
//                if(c0==null){
//                    logger.warn("got null for "+ "sumOfALLWords");
//                    return;
//                }
//                if(c1==null){
//                    logger.warn("got null for "+ w2);
//                    return;
//                }
//                if(c2==null){
//                    logger.warn("got null for "+ w1 + " " + w2);
//                    return;
//                }
//
//                double k2 = (Math.log10(n2+1)+1)/(Math.log10(n2+1)+2);
//                double k3=(Math.log10(n3+1)+1)/(Math.log10(n3+1)+2);
//
//                double prob=k3*(n3/c2)+(1-k3)*k2*(n2/c1)+(1-k3)*(1-k2)*(n1/c0);
//                //logger.info("%%%% SUCCESSS one");
//                context.write(key, new Text(Double.toString(prob)));
//            }catch (NullPointerException exp){
//                logger.warn("got exp: i=" +expCounter++ +"\n");
//            }


        }

        public void cleanup(Context context) throws IOException {

            //todo: this has some useful code example to write logs into hdfs
//            FileSystem fs=FileSystem.get(context.getConfiguration());
//            FSDataOutputStream fin = fs.create(new Path("/meniAdler"));

            logger.info("singlesMap.size: " + singlesMap.size() + "\n");
            // logger.info( "pairsMap.size: "+ pairsMap.size()+"\n");
//            fin.writeUTF(done);
//            fin.close();
        }
    }
    //todo: Change IntWriteable into LongWriteable, since we are dealing with word counting.


    public static class Map1 extends Mapper<Text, IntWritable, Text, IntWritable> {
        private Logger logger = org.apache.log4j.Logger.getLogger(Map1.class);

        public void setup(Context context) {
        }

        public void map(Text key, IntWritable value, Context context) throws IOException, InterruptedException {
            String[] ngram_words = key.toString().split(" ");

            context.write(new Text(ngram_words[0]), value);
            context.write(new Text(ngram_words[1]), value);
            context.write(new Text(ngram_words[2]), value);
            context.write(new Text("sumOfALLWords"), new IntWritable(3 * value.get()));

        }
    }

    public static class Map2 extends Mapper<Text, IntWritable, Text, TupleWritable> {

        public void map(Text key, IntWritable value, Context context) throws IOException, InterruptedException {

            String[] ngram_words = key.toString().split(" ");

            TupleWritable value_Key = new TupleWritable(new Writable[]{value, key});

            context.write(new Text(ngram_words[0] + " " + ngram_words[1]), value_Key);
            context.write(new Text(ngram_words[1] + " " + ngram_words[2]), value_Key);
        }
    }

    public static class Reducer2 extends Reducer<Text, TupleWritable, Text, IntWritable> {
        Set<Text> textSet;

        public void setup(Context context) {
            textSet = new HashSet<Text>();
        }

        public void reduce(Text key, Iterable<TupleWritable> values, Context context) throws IOException, InterruptedException {

            int sum = 0;
            for (TupleWritable tuple : values) {
                IntWritable value = (IntWritable) tuple.get(0);
                sum += value.get();
                Text gram3 = (Text) tuple.get(1);
                if (!textSet.contains(gram3)) {
                    textSet.add(gram3);
                }
            }
            //this will write all the triplets which are related to the same key- which is a pair
            for (Text gram3 : textSet) {
                //note for splitter in the mapper, first we get the key+ 3gram origin.
                context.write(new Text(key.toString() + " " + gram3.toString()), new IntWritable(sum));
            }

            textSet.clear();
            context.write(key, new IntWritable(sum));
        }
    }

    //TODO: To added to init class for data.
    public static class Map3 extends Mapper<Text, IntWritable, Text, IntWritable> {

        public void map(Text key, IntWritable value, Context context) throws IOException, InterruptedException {

            context.write(key, value);
        }
    }


    public static class ReduceSumValue extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

}