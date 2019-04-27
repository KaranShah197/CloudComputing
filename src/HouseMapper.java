/**
 * @author Karan Shah
 *
 */

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class HouseMapper extends Mapper<LongWritable, Text, Text, Text> {

    public void map(LongWritable mapperKey, Text mapperValue, Context mapperContext) {

        try {
            if(mapperKey.get() != 0) {
                String[] data = mapperValue.toString().split("\t");
                mapperContext.write(new Text(data[1]), new Text(data[2]));
            } else {
                return;
            }
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }
}