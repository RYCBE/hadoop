
import org.ansj.splitWord.analysis.DicAnalysis;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.*;
import java.util.HashMap;
import java.util.List;
import java.util.HashSet;

import org.ansj.domain.Term;
import org.ansj.library.DicLibrary;

public class PreSolve {

    public static class PreSolveMapper extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);

        public void setup(Context context) {
            try {
                Path pt = new Path("/home/rycbe/final/task2/person_name_list.txt");
                Configuration tmpconf = new Configuration();
                tmpconf.addResource("home/rycbe/hadoop_installs/hadoop-2.7.7/etc/hadoop/core-site/xml");
                tmpconf.addResource("home/rycbe/hadoop_installs/hadoop-2.7.7/etc/hadoop/hdfs-site/xml");
                FileSystem fs = FileSystem.get(tmpconf);

                BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
                String line;
                line = br.readLine();
                while (line != null) {
                    DicLibrary.insert(DicLibrary.DEFAULT, line);
                    line = br.readLine();
                }

            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            List<Term> terms = DicAnalysis.parse(line).getTerms();
            HashSet<String> termSet = new HashSet<>();
            for (Term tmp : terms) {
                if (tmp.getNatureStr().equals("userDefine")) {
                    termSet.add(tmp.getName());
                }
            }
            StringBuffer posting = new StringBuffer();
            for (String tmp : termSet) {
                posting.append(tmp);
                posting.append(" ");
            }
            if (posting.length() > 0) {
                posting.deleteCharAt(posting.length() - 1);
                String post = posting.toString();
                context.write(new Text(post), one);
            }
        }
    }

    public static class PreSolveReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable i : values)
                sum += i.get();
            context.write(key, new IntWritable(sum));
        }
    }
}
