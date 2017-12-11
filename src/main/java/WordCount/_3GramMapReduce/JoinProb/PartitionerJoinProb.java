package WordCount._3GramMapReduce.JoinProb;

import WordCount.Writable.PairWritableInteger;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class PartitionerJoinProb extends Partitioner<Text,PairWritableInteger> {

    @Override
    public int getPartition(Text text, PairWritableInteger pairWritableInteger, int numReduceTasks) {
        String gram3=text.toString();
        return (gram3.hashCode() & Integer.MAX_VALUE) % numReduceTasks;
    }
}