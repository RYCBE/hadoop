import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

public class DoubleArray implements Writable {
    private double[] data;

    public DoubleArray() {
    }
    public DoubleArray(double[] data) {
        set(data);
    }
    public void set(double[] data) {
        this.data = data;
    }

    public void set(int i, double data) {
        this.data[i] = data;
    }

    public double[] get() {
        return data;
    }

    public void write(DataOutput out) throws IOException {
        int length = 0;
        if (data != null) {
            length = data.length;
        }
        out.writeInt(length);
        for (int i = 0; i < length; i++) {
            out.writeDouble(data[i]);
        }
    }

    public void readFields(DataInput in) throws IOException {
        int length = in.readInt();
        data = new double[length];
        for (int i = 0; i < length; i++) {
            data[i] = in.readDouble();
        }
    }
}