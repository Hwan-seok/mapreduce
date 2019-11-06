import java.io.IOException;
import java.util.*;
import java.util.AbstractMap.SimpleEntry;
import java.util.Map.Entry;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class MatrixMul {
//["a", 0, 0, 63]
    public static class Map extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            char mat = line.charAt(2);
            line=line.substring(6,line.length()-1);
            line=line.replace(" ","");
            String[] indicesAndValue = line.split(",");
            Text outputKey = new Text();
            Text outputValue = new Text();
            System.out.println(mat);

            if (mat=='a') {
                for(Integer i = 0 ; i < 5 ; i ++) {
                    outputKey.set(indicesAndValue[0] + "," + i.toString());
                    outputValue.set(indicesAndValue[1] + "," + indicesAndValue[2]);
                    System.out.println(outputKey.toString());
                    System.out.println(outputValue.toString());
                    context.write(outputKey, outputValue);
                }
            } else {
                for(Integer i = 0 ; i < 5 ; i ++) {
                    outputKey.set(i.toString() + "," + indicesAndValue[1]);
                    outputValue.set(indicesAndValue[0] + "," + indicesAndValue[2]);
                    System.out.println(outputKey.toString());
                    System.out.println(outputValue.toString());
                    context.write(outputKey, outputValue);
                }
            }
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String[] value;
            ArrayList<Entry<Integer, Integer>> list = new ArrayList<Entry<Integer, Integer>>();
            for (Text val : values) {
                value = val.toString().split(",");
                list.add(new SimpleEntry<Integer, Integer>(Integer.parseInt(value[0]), Integer.parseInt(value[1])));
            }
            System.out.println(key);

            System.out.println(list);

            int[] counter = new int[5];
            int[] result = new int[5];
            int finalResult = 0;
            for(Entry<Integer, Integer> set : list){
                int ke = set.getKey();
                float val = set.getValue();
                counter[ke]+=1;
                if(result[ke]==0){
                    result[ke]+=val;
                } else {
                    result[ke]*=val;
                }
        }
            for(int i = 0 ; i < 5 ; i++){
            if(counter[i]<2){
                result[i]=0;
            } else {
                finalResult+=result[i];
            }
        }
            context.write(key,new Text(""+finalResult));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "MatrixMul");
        job.setJarByClass(MatrixMul.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}