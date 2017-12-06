
import java.io.*;
import java.util.*;

import N_Gram.NgramInputFormat;
import N_Gram.SProbInputFormat;
import N_Gram.TripletsInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.jboss.netty.util.internal.ConcurrentHashMap;


import org.apache.log4j.Logger;

public class WordCount {

    //todo:change this
    final static String userPath = "/user/Administrator";

    public static void main(String[] args) throws Exception {


        String inputFile = "/dataHeb";
        String output=args[1];
        String output1 = "/output1";
        String output2 = "/output2";
        String output3 = "/output3";

        String outputSort="/outputSort";

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


        boolean isSuccess = getProbabilties(c, files[1]);
        if (!isSuccess) {
            System.exit(1);
        }

        //sort with another map reduce.
        Job j3 = new Job(c, "sort keys with prob");
        j3.setJarByClass(WordCount.class);
        j3.setMapperClass(MapSortProb.class);
        j3.setReducerClass(ReduceSortProb.class);
        j3.setMapOutputKeyClass(Text.class);
        j3.setMapOutputValueClass(PairWritableDouble.class);
        j3.setOutputKeyClass(Text.class);
        j3.setOutputValueClass(DoubleWritable.class);
        j3.setInputFormatClass(SProbInputFormat.class);
        NgramInputFormat.addInputPath(j3, new Path(output)); //todo: refractor here
        FileOutputFormat.setOutputPath(j3, new Path (outputSort));


        System.exit( j3.waitForCompletion(true)? 0 : 1);
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
//todo: see why tupleWriteable in the reduce is all empty.
        j2.setMapOutputKeyClass(Text.class);
        j2.setMapOutputValueClass(PairWritableInteger.class);

//        j2.setOutputKeyClass(Text.class);
//        j2.setOutputValueClass(IntWritable.class);
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

    public static boolean getProbabilties(Configuration c,String output) throws IOException, ClassNotFoundException, InterruptedException {
        String tripletsInput = "/output3/part-r-00000";
        String pairsInput = "/output2/part-r-00000";
        //todo: add 2 inputs files here:
        // output 3 and output 2
        Job j3 = new Job(c, "Join triplets with pairs - calc prob");
        MultipleInputs.addInputPath(j3, new Path(tripletsInput), TripletsInputFormat.class, MapperJoinProbTriplet.class);
        MultipleInputs.addInputPath(j3, new Path(pairsInput), TripletsInputFormat.class, MapperJoinProbPair.class);


        j3.setJarByClass(WordCount.class);
        j3.setReducerClass(ReducerJoinProb.class);
        j3.setOutputKeyClass(Text.class);
        j3.setMapOutputKeyClass(Text.class);
        j3.setMapOutputValueClass(PairWritableInteger.class);

        j3.setOutputValueClass(DoubleWritable.class);
        //j3.setInputFormatClass(Multip.class);
        TripletsInputFormat.addInputPath(j3, new Path(tripletsInput));
        FileOutputFormat.setOutputPath(j3, new Path(output));


        return j3.waitForCompletion(true);
    }
    public static class MapperJoinProbPair extends Mapper<Text, IntWritable, Text, PairWritableInteger> {
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
    public static class MapperJoinProbTriplet extends Mapper<Text, IntWritable, Text, PairWritableInteger> {

        public void map(Text key, IntWritable value, Context context) throws IOException, InterruptedException {

            PairWritableInteger pair = new PairWritableInteger();
            pair.setSecond(value.get());
            context.write(key, pair);

        }
    }

    public static class ReducerJoinProb extends Reducer<Text, PairWritableInteger, Text, DoubleWritable> {

        private static ConcurrentHashMap<String, Integer> singlesMap = new ConcurrentHashMap<String, Integer>();

        private Logger logger = org.apache.log4j.Logger.getLogger(ReducerJoinProb.class);



        public void setup(Reducer.Context context) throws IOException {

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

        public void reduce(Text key, Iterable<PairWritableInteger> values, Context context) throws IOException, InterruptedException {

            logger.info("key::" +Arrays.toString(key.toString().getBytes()));
            String w1;
            String w2;
            String w3;
            int N1;
            int N2 = 0;
            int N3 = 0;
            int C0;
            int C1;
            int C2 = 0;
            //got 3gram key. //all values here are related to the same 3gram
            //todo: how do we get the 3gram word count? maybe add it as a value to the output2
            // so a key would be
            // Danny went home , valOfDanny
            // Danny went home ,pair(Danny went, dannyWentCount)
            // Danny went home , went home, wentHomeCount.
            String gram3 = key.toString();
            String[] splittedGram3 = gram3.split(" ");

            for (PairWritableInteger pair : values) {
                String pairWord = pair.getFirst();

                if (pairWord.equals("")) { //got a key from triplet input
                    N3 += pair.getSecond();//todo: we love todo

                } else {
                    int pairsCount = pair.getSecond();
                    logger.info(pairsCount +"$$$$$");
                    logger.info(pairWord + "||"+gram3);
                    String[] splitedWords = pairWord.split(" ");
                    if (splitedWords[0].equals(splittedGram3[0]) && splitedWords[1].equals(splittedGram3[1])) {
                        //infix:  c2 = pairsMap.get(w1 + " " + w2);
                        C2 += pairsCount;

                    } else if (splitedWords[0].equals(splittedGram3[1]) && splitedWords[1].equals(splittedGram3[2])) {
                        //postfix  n2 = pairsMap.get(w2 + " " + w3);
                        logger.info("got to nCount in splitedWords");
                        N2 += pairsCount;
                    } else {
                        throw new NullPointerException("got bad login else., should not happen");
                    }
                }
            }
            String[] splited = key.toString().split(" ");
            w1 = splited[0];
            w2 = splited[1];
            w3 = splited[2];
            N1 = singlesMap.get(w3);
            C1 = singlesMap.get(w2);
            C0 = singlesMap.get("sumOfALLWords");
            logger.info("before calc: N2:"+N2 +" N3:"+N3+" C2:"+C2+" C1:"+C1+" C0:"+C0);

            double k2 = (Math.log10(N2 + 1) + 1) / (Math.log10(N2 + 1) + 2);
            double k3 = (Math.log10(N3 + 1) + 1) / (Math.log10(N3 + 1) + 2);
            double prob = k3 * (N3 / C2) + (1 - k3) * k2 * (N2 / C1) + (1 - k3) * (1 - k2) * (N1 / C0);
            context.write(key, new DoubleWritable(prob));
            logger.info("@@done write");


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

    public static class Map2 extends Mapper<Text, IntWritable, Text, PairWritableInteger> {
        private Logger logger = org.apache.log4j.Logger.getLogger(Map2.class);

        public void map(Text key, IntWritable value, Context context) throws IOException, InterruptedException {

            String[] ngram_words = key.toString().split(" ");
            PairWritableInteger value_Key = new PairWritableInteger(key.toString(), value.get());

            //logger.info("MAP2: " +((IntWritable) value_Key.get(0)).get());
            context.write(new Text(ngram_words[0] + " " + ngram_words[1]), value_Key);
            context.write(new Text(ngram_words[1] + " " + ngram_words[2]), value_Key);
        }
    }

    public static class Reducer2 extends Reducer<Text, PairWritableInteger, Text, IntWritable> {
        Set<String> textSet;
        private Logger logger = org.apache.log4j.Logger.getLogger(Reducer2.class);

        public void setup(Context context) {
            textSet = new HashSet<String>();
        }

        public void reduce(Text key, Iterable<PairWritableInteger> values, Context context) throws IOException, InterruptedException {

            int sum = 0;

            for (PairWritableInteger pair : values) {
                int sumOfPairs = pair.getSecond();
                sum += sumOfPairs;
                String gram3 = pair.getFirst();
//                logger.info("value:"+value.get() );
                if (!textSet.contains(gram3)) {
                    logger.info("REDUCER2:: added " + gram3 + "to the wonderful set N:" + sum);
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

            for (PairWritableDouble value : values) {
                valueList.add(value);
            }

            Collections.sort(valueList );
            for(PairWritableDouble pair:valueList){
                context.write(new Text(key.toString() +" "+pair.getFirst()),new DoubleWritable(pair.getSecond()) );
            }

        }
    }

}