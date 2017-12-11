package WordCount.Writable;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class PairWritableDouble implements WritableComparable<PairWritableDouble> {

    private String first;
    private Double second;

    public PairWritableDouble(String first, Double second) {
        this.first = first;
        this.second = second;
    }

    public PairWritableDouble() {
        this.first = "";
        this.second = -1.0;
    }
    //
    public String getFirst() {
        return first;
    }
    //
    public void setFirst(String key) {
        this.first = key;
    }
    //
    public Double getSecond() {
        return second;
    }

    public void setSecond(Double value) {
        this.second = value;
    }



    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(first);
        dataOutput.writeDouble(second);
    }

    public void readFields(DataInput dataInput) throws IOException {
        first=dataInput.readUTF();
        second=dataInput.readDouble();
    }

    public int compareTo(PairWritableDouble o) {
        if(o==null){
            throw new NullPointerException();
        }
//        if(this.first.compareTo(((String) o.first))>0){
//            return 1;
//        }
//        else
//        if(this.first.compareTo(((String) o.first))<0){
//            return -1;
//        }
//        else
        if(second>(Double) o.second){
            return 1;
        }
        else{
            if(second<(Double) o.second){
                return -1;
            }
            else{
                return 0;
            }
        }
    }
}

