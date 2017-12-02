package N_Gram;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

public class NgramRecordReader extends RecordReader <Text,IntWritable> {

    private LineRecordReader reader;

//    protected abstract  User parseUser(String str);
//    protected abstract UserAction parseUserAction(String str) throws IOException;

    NgramRecordReader() {
        reader = new LineRecordReader();
    }

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context)
            throws IOException, InterruptedException {
        reader.initialize(split, context);
    }


    @Override
    public void close() throws IOException {
        reader.close();
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        return reader.nextKeyValue();
    }

    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        return parseNGram(reader.getCurrentValue().toString());
    }



    @Override
    public IntWritable getCurrentValue() throws IOException, InterruptedException {
        return parseCount(reader.getCurrentValue().toString());
    }


    @Override
    public float getProgress() throws IOException, InterruptedException {
        return reader.getProgress();
    }


    //private methods:
    private Text parseNGram(String s) {
        String[] splitted = s.split("\t");
        if (splitted.length < 5) { return null; } /* malformed line, skip it. */ //TODO: deal
        String ngram = splitted[0];
        return new Text(ngram);
    }
    private IntWritable parseCount(String s){
        String[] splitted = s.split("\t");
        if (splitted.length < 5) { return null; } /* malformed line, skip it. */ //TODO: deal
        String count = splitted[2];
        return new IntWritable(Integer.valueOf(count));
    }

}