package WordCount;

import java.io.*;

import WordCount.N_Gram.*;
import WordCount._3GramMapReduce.GenerateOutputs.*;
import WordCount._3GramMapReduce.JoinCountProb.*;
import WordCount.Writable.*;
import WordCount._3GramMapReduce.SortProb;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.Logger;

/**
 * program flow:
 * from mainData
 *  1. compute from triplets all singles
 *  2. *** all pairs with triplet ref.
 *  3. all triplets
 *  ----
 *  stage2:
 *  map/reduce:
 *  1.load singles files into RAM (look for shared cluster improvement)
 *  2.with 2 and 3 triplets compute probablities into output4.
 *  3. with output4 last file/redoce to sort by probablity.
 */

public class WordCount {
    private static Logger logger=Logger.getLogger(WordCount.class);

    public static void main(String[] args) throws Exception {

        //todo: look here
        final boolean isBuildDataSet=true;
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
//        boolean isBuildDataSet = false;
//        if (files.length > 2) {
//            isBuildDataSet = files[2].contains("build");
//        }
//
//

        if (isBuildDataSet) {
            System.out.println("getting started with building data");
            logger.info("getting started with building data");
            if (!initData(c, files[0], files[1])) {
                System.exit(1);
            }
        }


        //when done open this:

        String sortedData="/outputJoinProb/part-r-00000";
        System.out.println("getting started with building probabilities");
        if (!getProbabilties(c, sortedData)) {
            System.exit(1);
        }
        //todo:really think about this:


        System.out.println("getting started with building probabilities");
        if(!sort3GramWithProb(c,sortedData,files[1])){
            System.exit(1);
        }


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
        j1.setInputFormatClass(SequenceFileInputFormat.class);
        SequenceFileInputFormat.addInputPath(j1,input);
        FileOutputFormat.setOutputPath(j1, output1);
        //todo: look for inputPath

        Job j2 = new Job(c, "job2");
        j2.setJarByClass(WordCount.class);
        j2.setMapperClass(Map2.class);
        j2.setReducerClass(Reducer2.class);
        j2.setMapOutputKeyClass(Text.class);
        j2.setMapOutputValueClass(PairWritableInteger.class);

        j2.setOutputKeyClass(Text.class);
        j2.setOutputValueClass(IntWritable.class);

        j1.setInputFormatClass(SequenceFileInputFormat.class);
        SequenceFileInputFormat.addInputPath(j1,input);
        FileOutputFormat.setOutputPath(j2, output2);

        Job j3 = new Job(c, "job3");
        j3.setJarByClass(WordCount.class);
        j3.setMapperClass(Map3.class);
        j3.setReducerClass(ReduceSumValue.class);
        j3.setOutputKeyClass(Text.class);
        j3.setOutputValueClass(IntWritable.class);

        j3.setInputFormatClass(SequenceFileInputFormat.class);
        SequenceFileInputFormat.addInputPath(j1,input);
        FileOutputFormat.setOutputPath(j2, output2);
        FileOutputFormat.setOutputPath(j3, output3);

        boolean status1 = j1.waitForCompletion(true);
        boolean status2 = j2.waitForCompletion(true);
        boolean status3 = j3.waitForCompletion(true);
//
        return (status1 && status2 && status3);
       // return status1;
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
    public static boolean sort3GramWithProb(Configuration c,String input,String output) throws IOException, ClassNotFoundException, InterruptedException {
        //sort with another map reduce.
        Job job = new Job(c, "sort keys with prob");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(SortProb.MapSortProb.class);
        job.setReducerClass(SortProb.ReduceSortProb.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(PairWritableDouble.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        job.setInputFormatClass(SProbInputFormat.class);
        NgramInputFormat.addInputPath(job, new Path(input)); //todo: refractor here
        FileOutputFormat.setOutputPath(job, new Path (output));
        return job.waitForCompletion(true);
    }



    /**
     * This is the last part of MapReduce.
     */


}