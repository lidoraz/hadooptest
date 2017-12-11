package WordCount.Writable;

import javafx.util.Pair;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class PairWritableInteger implements WritableComparable<Pair> {

    private String first;
    private Integer second;

    public PairWritableInteger(String first, Integer second) {
        this.first = first;
        this.second = second;
    }

    public PairWritableInteger() {
        this.first = "";
        this.second = -1;
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
    public int getSecond() {
        return second;
    }

    public void setSecond(Integer value) {
        this.second = value;
    }

    public int compareTo(Pair o) {
        if(o==null){
            throw new NullPointerException();
        }
        if(this.first.compareTo(((String) o.getKey()))>0){
            return 1;
        }
        else
            if(this.first.compareTo(((String) o.getKey()))<0){
                return -1;
            }
            else
                if(second>(Integer) o.getKey()){
                    return 1;
                }
                else{
                    if(second<(Integer) o.getKey()){
                        return -1;
                    }
                    else{
                        return 0;
                    }
                }
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(first);
        dataOutput.writeInt(second);
    }

    public void readFields(DataInput dataInput) throws IOException {
        first=dataInput.readUTF();
        second=dataInput.readInt();
    }
}

