package N_Gram;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

public class NgramInputFormat extends FileInputFormat <Text,IntWritable>{


    public RecordReader<Text, IntWritable> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        return new NgramRecordReader();
    }

//    public org.apache.hadoop.mapreduce.RecordReader<LongWritable, Text> createRecordReader(InputSplit inputSplit, org.apache.hadoop.mapreduce.TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
//        return null;
//    }
}
