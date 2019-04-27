/**
 * @author Karan Shah
 *
 */

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class HouseDataMain {
    @SuppressWarnings("deprecation")
    public static void main(String[] args) {
        Job job;
        try {
            job = new Job(new Configuration(), "Average Taxi Time");
            job.setJarByClass(HouseDataMain.class);
            job.setMapperClass(HouseMapper.class);
            job.setReducerClass(HouseReducer.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        } catch (ClassNotFoundException | IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }
}
