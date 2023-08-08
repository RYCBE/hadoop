import java.io.*;
import java.util.HashMap;
import java.util.LinkedList;

import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class LPA {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if(otherArgs.length != 3) {
            System.err.println("Usage: LPA <data> <namelist> <out>");
            System.exit(-1);
        }

        String input_data_path = otherArgs[0];
        String input_namelist_path = otherArgs[1];
        String output_dir = otherArgs[2];
        String output_result_dir = output_dir + "/result";

        String[] names = get_names(conf, input_namelist_path);

        String data_seq_path = output_dir + "/data.seq";
        String label_seq_path = output_dir + "/label.seq";
        write_seq(conf, names, input_data_path, data_seq_path, label_seq_path);

        run_job(conf, data_seq_path, label_seq_path, output_result_dir, names);
    }

    private static String[] get_names(Configuration conf, String path) throws IOException {
        FileSystem fs = FileSystem.get(conf);

        // read from origin file
        InputStream is = fs.open(new Path(path));
        InputStreamReader isr = new InputStreamReader(is);
        BufferedReader reader = new BufferedReader(isr);

        LinkedList<String> tmp = new LinkedList<String>();
        while (true) {
            String line = reader.readLine();
            if(line == null)
                break;
            tmp.add(line);
        }
        return tmp.toArray(new String[tmp.size()]);
    }

    private static void write_seq(Configuration conf, String[] names, String data_path, String data_seq_path, String label_seq_path) throws IOException {
        FileSystem fs = FileSystem.get(conf);

        // read from origin file
        InputStream is = fs.open(new Path(data_path));
        InputStreamReader isr = new InputStreamReader(is);
        BufferedReader reader = new BufferedReader(isr);

        // create matrix
        DoubleArray[] weights = new DoubleArray[names.length];
        for(int i = 0; i < names.length; i++)
            weights[i] = new DoubleArray(new double[names.length]);

        // for each line
        while (true) {
            String line = reader.readLine();
            if (line == null)
                break;
            else {
                String[] a = line.split("\t");
                int id = name2index(a[0], names);
                String[] links = a[1].split(";");
                for(String link: links) {
                    if(link.equals(""))
                        break;
                    String[] t = link.split(",");
                    weights[id].set(name2index(t[0], names), Double.parseDouble(t[1]));
                }
            }
        }

        reader.close();
        isr.close();
        is.close();

        // write data_seq
        fs.create(new Path(data_seq_path)).close();
        SequenceFile.Writer writer = new SequenceFile.Writer(fs, conf, new Path(data_seq_path), IntWritable.class, DoubleArray.class);
        for(int i = 0; i < names.length; i++)
            writer.append(new IntWritable(i), weights[i]);
        writer.close();

        // write label_seq
        fs.create(new Path(label_seq_path)).close();
        SequenceFile.Writer label_writer = new SequenceFile.Writer(fs, conf, new Path(label_seq_path), IntWritable.class, IntWritable.class);
        for(int i = 0; i < names.length; i++)
            label_writer.append(new IntWritable(i), new IntWritable(i));
        conf.set("labels", label_seq_path);
        label_writer.close();
    }

    private static int name2index(String name, String[] names) {
        for(int i = 0; i < names.length; i++) {
            if(name.equals(names[i]))
                return i;
        }
        return -1;
    }

    private static void set_job(Job job, String data_path, String output_dir) throws IOException {
        job.setJarByClass(LPA.class);
        // set mapper/reducer
        job.setMapperClass(LPMapper.class);
        job.setReducerClass(LPReducer.class);

        // set input/output class
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Weight.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        // set input/output path
        SequenceFileInputFormat.addInputPath(job, new Path(data_path));
        SequenceFileOutputFormat.setOutputPath(job, new Path(output_dir));
    }

    private static void run_job(Configuration conf, String data_seq_path, String label_seq_path, String output_result_dir, String[] names) throws IOException, InterruptedException, ClassNotFoundException {
        int cnt = 0;
        while(true) {
            Job job = new Job(conf, "LPA");

            String tmp = output_result_dir + "/tmp";
            String result_seq = tmp + "/part-r-00000";

            set_job(job, data_seq_path, tmp);

            job.waitForCompletion(true);

            /**
             * output_dir/data
             *           /data.seq
             *           /person_name_list.txt
             *           /person_name_list.txt.seq
             *           /result/result_at_x.txt
             *                  /tmp/part-r-00000
             */

            // output .txt file during iteration
            String result_i = output_result_dir + "/result_" + cnt + ".txt";
            write_label_to_text(conf, result_seq, result_i, names);

            cnt += 1;

            if(cnt < 10 && diff_labels(conf, result_seq, label_seq_path)) {
                // delete old result, copy new result to old result
                FileSystem fs = FileSystem.get(conf);
                fs.delete(new Path(label_seq_path));
                FileUtil.copy(fs, new Path(result_seq), fs, new Path(label_seq_path), false, conf);
                fs.delete(new Path(tmp));
            }
            else
                return;

        }
    }

    private static boolean diff_labels(Configuration conf, String new_label_seq_path, String old_label_seq_path) throws IOException {
        int[] labels_1 = get_label_from_seq(conf, new_label_seq_path);
        int[] labels_2 = get_label_from_seq(conf, old_label_seq_path);
        if(labels_1.length != labels_2.length)
            return true;
        for(int i = 0; i < labels_1.length; i++) {
            if(labels_1[i] != labels_2[i])
                return true;
        }
        return false;
    }

    private static void write_label_to_text(Configuration conf, String result_seq, String result_filename, String[] names) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        int[] index_label_pairs = get_label_from_seq(conf, result_seq);

        HashMap<Integer, LinkedList<String>> label_name_pairs = new HashMap<Integer, LinkedList<String>>();
        // label_name_pairs[label] == [name1, name2, ...]
        for(int i = 0; i < index_label_pairs.length; i++) {
            String name = names[i];
            int label = index_label_pairs[i];
            if(! label_name_pairs.containsKey(index_label_pairs[i])) {
                LinkedList<String> tmp = new LinkedList<String>();
                tmp.add(name);
                label_name_pairs.put(label, tmp);
            }
            else {
                label_name_pairs.get(label).add(name);
            }
        }

        StringBuilder sb = new StringBuilder();
        sb.append("Result\n\n");

        int label_index = 0;
        for(int label: label_name_pairs.keySet()) {
            sb.append("Label_");
            sb.append(label_index);
            sb.append(":\n");

            for(String name: label_name_pairs.get(label)) {
                if(!name.equals(label_name_pairs.get(label).get(0)))
                    sb.append(", ");
                sb.append(name);
            }
            sb.append("\n\n");
            label_index += 1;
        }

        FSDataOutputStream fout = fs.create(new Path(result_filename), true);
        fout.write(sb.toString().getBytes());
        fout.close();
    }

    private static int[] get_label_from_seq(Configuration conf, String label_seq_path) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        SequenceFile.Reader reader = new SequenceFile.Reader(fs, new Path(label_seq_path), conf);
        IntWritable index = new IntWritable();
        IntWritable label = new IntWritable();
        LinkedList<Integer> labels = new LinkedList<Integer>();
        while(reader.next(index, label)) {
            labels.add(label.get());
        }

        int[] tmp = new int[labels.size()];
        int i = 0;
        for(int l: labels) {
            tmp[i] = l;
            i += 1;
        }
        return tmp;
    }
}