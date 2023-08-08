import org.ansj.library.DicLibrary;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;

public class WordPair {
    public static class WordPairMapper extends Mapper<Object, Text, Text, IntWritable> {
        private Text wordPair = new Text();
        private static HashMap<String, Integer> H = new HashMap<String, Integer>();

        public void setup(Context context) {
            try {
                Configuration tmpConf = new Configuration();
                FileSystem fs = FileSystem.get(tmpConf);

                Path sameperson = new Path("/home/rycbe/final/sameperson.txt");
                BufferedReader samebr = new BufferedReader(new InputStreamReader(fs.open(sameperson)));
                String line = samebr.readLine();
                while (line != null) {
                    String[] tmp = line.split(",");
                    H.put(tmp[0],Integer.parseInt(tmp[1]));
                    line = samebr.readLine();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        public void map(Object key,Text value,Context context) throws IOException,InterruptedException{
            String[] tmp = value.toString().split(",");
            IntWritable count = new IntWritable(Integer.parseInt(tmp[1]));
            String[] words = tmp[0].split(" ");

            for(int i=0;i<words.length;++i){
                for(int j=0;j<words.length;++j){
                    if(words[i].equals(words[j]) || H.get(words[i])==H.get(words[j])) continue;
                    wordPair.set("<"+words[i]+","+words[j]+">");
                    context.write(wordPair,count);
                }
            }
        }
    }

    public static class WordPairReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }
}
