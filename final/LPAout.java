import LPA.DoubleArray;
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

public class LPAout {

    public static class LPAoutMapper extends Mapper<Text, Text, Text, Text> {
        private String[] namelist = new String[885];
        private DoubleArray[] matrix = new DoubleArray[885];

        private int name2index(String name) {
            for(int i = 0; i < namelist.length; i++) {
                if(name.equals(namelist[i]))
                    return i;
            }
            return -1;
        }

        public void setup(Context context) throws IOException, InterruptedException{
            //get namelist;
            super.setup(context);
            try {
                Path pt = new Path("/home/rycbe/final/task2/person_name_list.txt");
                Configuration tmpconf = new Configuration();
                FileSystem fs = FileSystem.get(tmpconf);

                BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
                String line;
                line = br.readLine();
                int i = 0;
                while (line != null) {
                    namelist[i] = line;
                    line = br.readLine();
                    i++;
                }
            } catch (Exception e) {
                e.printStackTrace();
            }

            //init matrix
            for(int i=0;i<885;++i){
                double[] tmpdata = new double[885];
                matrix[i] = new DoubleArray(tmpdata);
            }
            //get matrix
            try {
                Configuration conf = context.getConfiguration();
                Path pt = new Path("/home/rycbe/final/LPAGraph_"+10+"/part-r-00000");
                FileSystem fs = FileSystem.get(conf);

                BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
                String line;
                line = br.readLine();
                while (line != null) {
                    String[] kv = line.split("\t");
                    int kindex = Integer.parseInt(kv[0]);


                    String[] links = kv[1].split(" ");
                    for(int i=0;i<links.length;++i){
                        double s = Double.parseDouble(links[i]);

                        matrix[kindex].set(i,s);
                    }
                    line = br.readLine();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }

        }

        public void map(Text key, Text val, Context context)
                throws IOException, InterruptedException{

            int k = Integer.parseInt(key.toString());

            double[] mk = matrix[k].get();
            int vindex = 0;
            for(int i=1;i<mk.length;++i){
                if(mk[i]>mk[vindex]){
                    vindex = i;
                }
            }

            context.write(key,new Text(vindex+""));
        }
    }

//    public static class LPARYReducer extends Reducer<IntWritable, Text, IntWritable,Text> {
//        public void reduce(IntWritable key, Text val, Mapper.Context context)
//                throws IOException, InterruptedException {
//            context.write(key,val);
//        }
//    }

}
