package WordCount.N_Gram;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

public class SProbRecordReader extends RecordReader<Text, DoubleWritable> {


    private org.apache.hadoop.mapreduce.lib.input.LineRecordReader reader;

//    protected abstract  User parseUser(String str);
//    protected abstract UserAction parseUserAction(String str) throws IOException;

    SProbRecordReader() {
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
    public DoubleWritable getCurrentValue() throws IOException, InterruptedException {
        return parseCount(reader.getCurrentValue().toString());
    }


    @Override
    public float getProgress() throws IOException, InterruptedException {
        return reader.getProgress();
    }


    //private methods:
    private Text parseNGram(String s) {
        String[] splitted = s.split("\t");
        //todo: deal with malformed line ?
//        if (splitted.length < 5) { return null; } /* malformed line, skip it. */ //TODO: deal
        String ngram = splitted[0];
        int ngramLen = ngram.length();
        if (ngram.charAt(ngramLen - 1) == ' ') {
            return new Text(ngram.substring(0, ngramLen - 1));
        } else {
            return new Text(ngram);
        }
    }

    private DoubleWritable parseCount(String s) {
        String[] splitted = s.split("\t");
//        if (splitted.length < 5) { return null; } /* malformed line, skip it. */ //TODO: deal
        String count = splitted[1];
        return new DoubleWritable(Double.valueOf(count));
    }


}
