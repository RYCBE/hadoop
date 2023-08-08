import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class PR {
    public static class BuildGraph extends Mapper<Text,Text,Text, Text>{
        public void map(Text key,Text val,Context context) throws IOException, InterruptedException{
            context.write(key,new Text("1.0;"+val.toString()));
        }
    }
    public static class PRMapper extends Mapper<Text,Text,Text, Text>{
        public void map(Text key,Text val,Context context) throws IOException, InterruptedException{
            String sval = val.toString();
            String[] links = sval.split(";");
            double rank = Double.parseDouble(links[0]);
            for(int i = 1;i<links.length;++i){
                String[] tuple = links[i].split(",");
                context.write(new Text(tuple[0]), new Text(rank*Double.parseDouble(tuple[1])+""));
            }
            context.write(key,new Text(sval.substring(sval.indexOf(";")+1)));
        }
    }

    public static class PRReducer extends Reducer<Text,Text,Text,Text>{
        public void reduce(Text key,Iterable<Text> vals,Context context) throws IOException, InterruptedException{
            double PR = 0.0;
            String res = "";
            for(Text val:vals){
                String tmp = val.toString();
                if(tmp.indexOf(";")!=-1){
                    res = tmp;
                }
                else {
                    PR += Double.parseDouble(tmp);
                }
            }
            PR = 0.15+0.85*PR;
            context.write(new Text(key),new Text(PR+";"+res));
        }
    }

    public static class PRViewer extends Mapper<Text,Text, FloatWritable,Text>{
        public void map(Text key,Text val,Context context) throws IOException,InterruptedException{
            String[] tmp = val.toString().split(";");
            context.write(new FloatWritable(Float.parseFloat(tmp[0])),new Text(key));
        }
    }
}
