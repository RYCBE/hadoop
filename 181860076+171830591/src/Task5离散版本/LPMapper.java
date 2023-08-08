import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.LinkedList;

public class LPMapper extends Mapper<IntWritable, DoubleArray, IntWritable, Weight> {
    private int[] labels;
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        Configuration conf = context.getConfiguration();
        String labels_path = conf.get("labels");

        // read labels from fs
        FileSystem fs =  FileSystem.get(conf);
        Path path = new Path(labels_path);
        SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, conf);
        IntWritable key = new IntWritable();
        IntWritable value = new IntWritable();

        LinkedList<Integer> tmp = new LinkedList<Integer>();
        while (reader.next(key, value)) {
            int index = key.get();
            int label = value.get();
            tmp.add(label);
        }
        this.labels = new int[tmp.size()];
        int i = 0;
        for(Integer label: tmp) {
            this.labels[i] = label;
            i += 1;
        }
        IOUtils.closeStream(reader);
    }

    public void map(IntWritable key, DoubleArray value, Context context)
            throws IOException, InterruptedException {
        for(int i = 0; i < value.get().length; i++) {
            double weight = value.get()[i];
            int label = this.labels[i];
            context.write(key, new Weight(label, weight));
        }
        // for each neighbor i in WeightArray
        //     send(key, Wright(labels[i], value[i])
    }

}