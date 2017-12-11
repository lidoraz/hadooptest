package WordCount.N_Gram;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

public class SProbInputFormat extends FileInputFormat <Text,DoubleWritable>{


    public RecordReader<Text, DoubleWritable> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        return new SProbRecordReader();
    }

//    public org.apache.hadoop.mapreduce.RecordReader<LongWritable, Text> createRecordReader(InputSplit inputSplit, org.apache.hadoop.mapreduce.TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
//        return null;
//    }
}
