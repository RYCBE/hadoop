import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;

public class LPReducer extends Reducer<IntWritable, Weight, IntWritable, IntWritable> {

    public void reduce(IntWritable key, Iterable<Weight> value, Context context)
            throws IOException, InterruptedException {
        int label = -1;
        double max_weight = -1;
        HashMap<Integer, Double> weighted_labels = new HashMap<Integer, Double>();
        for(Weight w: value) {
            if(!weighted_labels.containsKey(w.get_label())) {
                weighted_labels.put(w.get_label(), w.get_weight());
            }
            else {
                weighted_labels.replace(w.get_label(), weighted_labels.get(w.get_label()) + w.get_weight());
            }
            if(weighted_labels.get(w.get_label()) > max_weight) {
                label = w.get_label();
                max_weight = w.get_weight();
            }
        }
        context.write(key, new IntWritable(label));
    }
}