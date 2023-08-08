import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

public class Weight implements Writable {
    private int label;
    private double weight;

    public Weight() {
    }
    public Weight(int label, double weight) {
        set(label, weight);
    }
    public void set(int label, double weight) {
        this.label = label;
        this.weight = weight;
    }
    public int get_label() {
        return this.label;
    }
    public double get_weight() {
        return this.weight;
    }

    public void write(DataOutput out) throws IOException {
        out.writeInt(this.label);
        out.writeDouble(this.weight);
    }

    public void readFields(DataInput in) throws IOException {
        this.label = in.readInt();
        this.weight = in.readDouble();
    }
}