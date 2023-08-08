import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Main {
    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();

//        Configuration conf1 = new Configuration();
//        conf1.set("mapred.textoutputformat.separator", ",");
//        Job preSolveJob = Job.getInstance(conf1, "lab1.PreSolve Job");
//        preSolveJob.setJarByClass(Main.class);
//        preSolveJob.setMapperClass(PreSolve.PreSolveMapper.class);
//        preSolveJob.setReducerClass(PreSolve.PreSolveReducer.class);
//        preSolveJob.setMapOutputKeyClass(Text.class);
//        preSolveJob.setMapOutputValueClass(IntWritable.class);
//        preSolveJob.setOutputKeyClass(Text.class);
//        preSolveJob.setOutputValueClass(IntWritable.class);
//        FileInputFormat.addInputPath(preSolveJob, new Path("/home/rycbe/final/harry_potter"));
//        FileOutputFormat.setOutputPath(preSolveJob, new Path("/home/rycbe/final/output"));
//        preSolveJob.waitForCompletion(true);

//        Job cooccurrenceJob = Job.getInstance(conf, "lab2 Job");
//        cooccurrenceJob.setJarByClass(Main.class);
//        cooccurrenceJob.setMapperClass(WordPair.WordPairMapper.class);
//        cooccurrenceJob.setReducerClass(WordPair.WordPairReducer.class);
//        cooccurrenceJob.setMapOutputKeyClass(Text.class);
//        cooccurrenceJob.setMapOutputValueClass(IntWritable.class);
//        cooccurrenceJob.setOutputKeyClass(Text.class);
//        cooccurrenceJob.setOutputValueClass(IntWritable.class);
//        FileInputFormat.addInputPath(cooccurrenceJob, new Path("/home/rycbe/final/output"));
//        FileOutputFormat.setOutputPath(cooccurrenceJob, new Path("/home/rycbe/final/output2"));
//        cooccurrenceJob.waitForCompletion(true);


//        Job RelationGraphics = Job.getInstance(conf, "RG");
//        RelationGraphics.setJarByClass(Main.class);
//        RelationGraphics.setInputFormatClass(KeyValueTextInputFormat.class);
//        RelationGraphics.setMapperClass(Normalizationer.NormalizationerMapper.class);
//        RelationGraphics.setReducerClass(Normalizationer.NormalizationerReducer.class);
//        RelationGraphics.setMapOutputValueClass(Text.class);
//        RelationGraphics.setMapOutputKeyClass(Text.class);
//        RelationGraphics.setOutputKeyClass(Text.class);
//        RelationGraphics.setOutputValueClass(Text.class);
//        FileInputFormat.addInputPath(RelationGraphics, new Path("/home/rycbe/final/output2"));
//        FileOutputFormat.setOutputPath(RelationGraphics,new Path("/home/rycbe/final/output3"));
//        RelationGraphics.waitForCompletion(true);

//        Job PRTask1 = Job.getInstance(conf,"PRTask");
//        PRTask1.setJarByClass(Main.class);
//        PRTask1.setInputFormatClass(KeyValueTextInputFormat.class);
//        PRTask1.setMapperClass(PR.BuildGraph.class);
//        PRTask1.setMapOutputValueClass(Text.class);
//        PRTask1.setMapOutputKeyClass(Text.class);
//        PRTask1.setOutputKeyClass(Text.class);
//        PRTask1.setOutputValueClass(Text.class);
//        FileInputFormat.addInputPath(PRTask1, new Path("/home/rycbe/final/output3"));
//        FileOutputFormat.setOutputPath(PRTask1,new Path("/home/rycbe/final/output4_0"));
//        PRTask1.waitForCompletion(true);
//
//
//        for(int i=0;i<10;++i){
//            Job PRTask2 = Job.getInstance(conf,"PRTask2");
//            PRTask2.setJarByClass(Main.class);
//            PRTask2.setInputFormatClass(KeyValueTextInputFormat.class);
//            PRTask2.setMapperClass(PR.PRMapper.class);
//            PRTask2.setReducerClass(PR.PRReducer.class);
//            PRTask2.setMapOutputValueClass(Text.class);
//            PRTask2.setMapOutputKeyClass(Text.class);
//            PRTask2.setOutputKeyClass(Text.class);
//            PRTask2.setOutputValueClass(Text.class);
//            FileInputFormat.addInputPath(PRTask2, new Path("/home/rycbe/final/output4_"+i));
//            FileOutputFormat.setOutputPath(PRTask2, new Path("/home/rycbe/final/output4_"+(i+1)));
//            PRTask2.waitForCompletion(true);
//        }

        Job PRTask3 = Job.getInstance(conf,"PRTask3");
        PRTask3.setJarByClass(Main.class);
        PRTask3.setInputFormatClass(KeyValueTextInputFormat.class);
        PRTask3.setMapperClass(PR.PRViewer.class);
        PRTask3.setMapOutputKeyClass(FloatWritable.class);
        PRTask3.setMapOutputValueClass(Text.class);
        PRTask3.setOutputKeyClass(Text.class);
        PRTask3.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(PRTask3, new Path("/home/rycbe/final/output4_10"));
        FileOutputFormat.setOutputPath(PRTask3, new Path("/home/rycbe/final/output4_final"));
        PRTask3.waitForCompletion(true);




    }
}
