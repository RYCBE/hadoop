import LPA.DoubleArray;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class LPAGraph {

    public static class LPAGraphBuilder extends Mapper<Text, Text, Text, Text> {
        private String[] namelist = new String[885];
        private DoubleArray[] matrix = new DoubleArray[885];

        private int name2index(String name) {
            for(int i = 0; i < namelist.length; i++) {
                if(name.equals(namelist[i]))
                    return i;
            }
            return -1;
        }

        public void setup(Context context){
            //get namelist;
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
                Path pt = new Path("/home/rycbe/final/output3/part-r-00000");
                Configuration tmpconf = new Configuration();
                FileSystem fs = FileSystem.get(tmpconf);

                BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
                String line;
                line = br.readLine();
                while (line != null) {
                    String[] kv = line.split("\t");
                    int kindex = name2index(kv[0]);


                    String[] links = kv[1].split(";");
                    for(int i=0;i<links.length;++i){
                        String[] linkikv = links[i].split(",");
                        int vindex = name2index(linkikv[0]);
                        double s = Double.parseDouble(linkikv[1]);

                        matrix[kindex].set(vindex,s);
                    }
                    line = br.readLine();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }

        }

        public void map(Text key, Text val, Context context)
                throws IOException, InterruptedException{

            for(int i=0;i<matrix.length;++i){
                context.write(new Text(""+i),new Text(matrix[i].toString()));
            }
        }
    }

}
