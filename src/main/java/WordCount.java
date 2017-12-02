
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.net.URI;
import java.rmi.UnexpectedException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.logging.FileHandler;

import N_Gram.NgramInputFormat;
import N_Gram.TripletsInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileAsTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.jboss.netty.util.internal.ConcurrentHashMap;



import org.apache.log4j.Logger;

public class WordCount {

    //todo:change this
    final static String userPath="/user/Administrator";

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
         * output2 - pairs
         * output3 - triplets
         */


        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        //TODO: צריך לראות משהו עם האינפוט, זה לא דוחף ישירות את האגרומנים לתוך הפאת'

        //go over all the corpus, generate triples,pairs,and single table with all occurrences.
        boolean isBuildDataSet=false;
        if(files.length>2){
            isBuildDataSet=files[2].contains("build");
        }


       if(isBuildDataSet){
           boolean isSuccess=initData(c,files[0],files[1]);
           if(!isSuccess){
               System.exit(1);
           }
       }


       String tripletsInput="/output3/part-r-00000";

        Job j3 = new Job(c, "last JOB");
        j3.setJarByClass(WordCount.class);
        j3.setMapperClass(Mapper1.class);
        j3.setReducerClass(Reducer1.class);
        j3.setOutputKeyClass(Text.class);
        j3.setOutputValueClass(IntWritable.class);
        j3.setInputFormatClass(TripletsInputFormat.class);
        TripletsInputFormat.addInputPath(j3, new Path(tripletsInput));
        FileOutputFormat.setOutputPath(j3,  new Path(files[1]));

//        Job job = Job.getInstance(new Configuration());
//        job.addCacheFile(new URI ("/path/to/file.csv"));

        System.exit(j3.waitForCompletion(true)?0:1);
    }

    public static boolean initData(Configuration c,String in,String output) throws IOException, ClassNotFoundException, InterruptedException {

        Path input = new Path(in);
        Path output1 = new Path(output+ "1");
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
        j2.setReducerClass(ReduceSumValue.class);
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

    public static class Mapper1 extends Mapper<Text, IntWritable, Text, IntWritable> {
        private Logger logger = org.apache.log4j.Logger.getLogger(Mapper1.class);
        public void map(Text key, IntWritable value, Context context) throws IOException, InterruptedException {
            context.write(key, value);
        }
    }

    public static class Reducer1 extends Reducer<Text, IntWritable, Text, Text> {

        private static ConcurrentHashMap<String,Integer> pairsMap=new ConcurrentHashMap<String, Integer>();
        private static ConcurrentHashMap<String,Integer> singlesMap=new ConcurrentHashMap<String, Integer>();

        private static StringBuilder output;

        private Logger logger = org.apache.log4j.Logger.getLogger(Reducer1.class);


        public void setup(Reducer.Context context) throws IOException {

            output=new StringBuilder();
            pairsMap=new ConcurrentHashMap<String, Integer>();
            singlesMap=new ConcurrentHashMap<String, Integer>();
            //load data

            try {

                FileSystem fs = FileSystem.get(context.getConfiguration());

                logger.info("########fs.getHomeDirectory() : "+fs.getHomeDirectory());
                FileStatus[] list=fs.listStatus(new Path("/"));
                logger.info("@@arrays.tostring::");
                logger.info(Arrays.toString(list));
                FSDataInputStream singlesInputStream =fs.open(new Path("output3/part-r-00000")); //singles//todo: wat
                FSDataInputStream pairsInputStream =fs.open(new Path("output2/part-r-00000")); //pairs

                BufferedReader reader = new BufferedReader(new InputStreamReader(singlesInputStream,"UTF8"));
                String line;
                String[] splitted;
                while ((line = reader.readLine()) != null) {
                    output.append("\n");
                    output.append(line + "\n");
                    splitted = line.split("\t");
                    singlesMap.put(splitted[0],Integer.valueOf(splitted[1]));
                }

                reader = new BufferedReader(new InputStreamReader(pairsInputStream,"UTF8"));
                while ((line = reader.readLine()) != null) {
                    splitted = line.split("\t");
                    pairsMap.put(splitted[0],Integer.valueOf(splitted[1]));
                }

                //System.out.println(out.toString());   //Prints the string content read from input stream
                reader.close();
                logger.info("logging");


            } catch(Exception e) {
                e.printStackTrace();
            }
        }
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {



            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }

            String[] splited = key.toString().split(" ");

//            logger.info("reduce:::"+ Arrays.toString(splited));
//            logger.info("singlesMap.size"+ singlesMap.size());
//            logger.info("pairsMap.size"+ pairsMap.size());

            Iterator e=singlesMap.keySet().iterator();
            for(int i=0 ; i<10;i++){

            }

           output.append(" $$parirsMAPP ");
             e=pairsMap.keySet().iterator();
            for(int i=0 ; i<10;i++){

            }

            try{
                String w1 = splited[0];
                String w2 = splited[1];
                String w3 = splited[2];
                int N1 = singlesMap.get(w3);
                int N2 = pairsMap.get(w2 + " " + w3);
                int N3 = sum;
                int c0 = singlesMap.get("sumOfALLWords");
                int c1 = singlesMap.get(w2);
                int c2 = pairsMap.get(w1 + " " + w2);
                double k2 = (Math.log10(N2+1)+1)/(Math.log10(N2+1)+2);
                double k3=(Math.log10(N3+1)+1)/(Math.log10(N3+1)+2);

                double prob=k3*(N3/c2)+(1-k3)*k2*(N2/c1)+(1-k3)*(1-k2)*(N1/c0);
                logger.info("%%%%%%% SUCCESSS one");
                context.write(key, new Text(Double.toString(prob)));
            }catch (Exception exp){
                logger.warn("got exp:");
                logger.warn(exp.getMessage());
            }


        }
        public void cleanup(Context context) throws IOException {
            FileSystem fs=FileSystem.get(context.getConfiguration());
            FSDataOutputStream fin = fs.create(new Path("/meniAdler"));

            fin.writeUTF(output.toString());
            fin.close();
        }
    }



    public static class Map1 extends Mapper<Text, IntWritable, Text, IntWritable> {

        public void map(Text key, IntWritable value, Context context) throws IOException, InterruptedException {

            String[] ngram_words = key.toString().split(" ");
            context.write(new Text(ngram_words[0]), value);
            context.write(new Text(ngram_words[1]), value);
            context.write(new Text(ngram_words[2]), value);
            context.write(new Text("sumOfALLWords"), new IntWritable(3 * value.get()));

        }
    }

    public static class Map2 extends Mapper<Text, IntWritable, Text, IntWritable> {

        public void map(Text key, IntWritable value, Context context) throws IOException, InterruptedException {

            String[] ngram_words = key.toString().split(" ");
            context.write(new Text(ngram_words[0] + " " + ngram_words[1]), value);
            context.write(new Text(ngram_words[1] + " " + ngram_words[2]), value);
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