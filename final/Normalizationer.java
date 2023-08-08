
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class Normalizationer {
    public static class NormalizationerMapper extends Mapper<Text,Text,Text,Text>{
        public void map(Text key, Text value, Context context)
            throws IOException, InterruptedException{
            String pair = key.toString();
            String val = value.toString();

            String first = pair.substring(1,pair.indexOf(","));
            String second = pair.substring(pair.indexOf(",")+1, pair.indexOf(">"));

            context.write(new Text(first),new Text(second+","+val));
        }
    }

    public static class NormalizationerReducer extends Reducer<Text,Text,Text,Text>{
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
            double sum = 0;
            StringBuilder res = new StringBuilder();
            for(Text val:values){
                String[] sval = val.toString().split(",");
                sum += Integer.parseInt(sval[1]);
                res.append(val.toString());
                res.append(";");
            }
            String[] tmp = res.toString().split(";");
            StringBuilder rres = new StringBuilder();
            for(String s:tmp){
                String[] t = s.split(",");
                rres.append(t[0]+","+(double)Integer.parseInt(t[1])/sum );
                rres.append(";");
            }
            context.write(new Text(key),new Text(rres.toString()));
        }
    }


}
