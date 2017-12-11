package WordCount._3GramMapReduce.JoinProb;

import WordCount.Writable.PairWritableInteger;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;
import org.jboss.netty.util.internal.ConcurrentHashMap;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;

public class ReducerJoinProb extends Reducer<Text, PairWritableInteger, Text, DoubleWritable> {

    private static ConcurrentHashMap<String, Integer> singlesMap = new ConcurrentHashMap<String, Integer>();

    private Logger logger = org.apache.log4j.Logger.getLogger(ReducerJoinProb.class);

    public void setup(Reducer.Context context) throws IOException {

        //todo: add make the hashmap sharable among other reducers, so it wont be created over and over
        // use : https://hadoop.apache.org/docs/r2.7.2/api/org/apache/hadoop/filecache/DistributedCache.html
        singlesMap = new ConcurrentHashMap<String, Integer>();
        //load data
        FileSystem fs = FileSystem.get(context.getConfiguration());
        //todo: debug here: add constants file with all paths.


        //FSDataInputStream singlesInputStream = fs.open(new Path("/output/1/")); //singles
        RemoteIterator<LocatedFileStatus> iterator=fs.listFiles(new Path("/output/1/"),false);
        int countPartIdx=0;
        int sumAllEntries=0;
        while(iterator.hasNext()){
            LocatedFileStatus fileStatus=iterator.next();
            //filter only part-r files.
            if (!fileStatus.getPath().getName().contains("part-r-")){
                continue;
            }
            FSDataInputStream singlesInputStream = fs.open(fileStatus.getPath()); //open a file
            BufferedReader reader = new BufferedReader(new InputStreamReader(singlesInputStream, "UTF-8"));
            String line;
            String[] splited;
            int countEntries=0;
            while ((line = reader.readLine()) != null) {
                splited = line.split("\t");
                singlesMap.put(splited[0], Integer.valueOf(splited[1]));
                countEntries++;
            }
            System.out.println("countPartIdx:"+countPartIdx+"|countEntires:"+countEntries);
            sumAllEntries+=countEntries;
            countPartIdx++;
            reader.close();
        }

        System.out.println("All sumAllEntries:"+sumAllEntries +"|singlesMap.size():"+singlesMap.size());

        logger.info("-reducer1: finish setup");

    }

    public void reduce(Text key, Iterable<PairWritableInteger> values, Context context) throws IOException, InterruptedException {

        logger.info("key::" + Arrays.toString(key.toString().getBytes()));
        String w1;
        String w2;
        String w3;
        double N1=0;
        double N2 = 0;
        double N3 = 0;
        double C0=0;
        double C1=0;
        double C2 = 0;
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
                N3 += pair.getSecond();

            } else {
                int pairsCount = pair.getSecond();
                String[] splitedWords = pairWord.split(" ");
                if (splitedWords[0].equals(splittedGram3[0]) && splitedWords[1].equals(splittedGram3[1])) {
                    //infix:  c2 = pairsMap.get(w1 + " " + w2);
                    C2 += pairsCount;

                } else if (splitedWords[0].equals(splittedGram3[1]) && splitedWords[1].equals(splittedGram3[2])) {
                    //postfix  n2 = pairsMap.get(w2 + " " + w3);

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
        double k2 = (Math.log10(N2 + 1) + 1) / (Math.log10(N2 + 1) + 2);
        double k3 = (Math.log10(N3 + 1) + 1) / (Math.log10(N3 + 1) + 2);
        double prob = k3 * (N3 / C2) + (1 - k3) * k2 * (N2 / C1) + (1 - k3) * (1 - k2) * (N1 / C0);
        if(prob==0.0){
            System.out.println("got 0.0 prob: "+key+ "|before calc:N1:"+N1+" N2:"+N2 +" N3:"+N3+" C2:"+C2+" C1:"+C1+" C0:"+C0+"\n");
        }
        context.write(key, new DoubleWritable(prob));



    }

    public void cleanup(Context context) throws IOException {
        singlesMap.clear();
        //todo: this has some useful code example to write logs into hdfs
    }
}
//todo: Change IntWriteable into LongWriteable, since we are dealing with word counting.