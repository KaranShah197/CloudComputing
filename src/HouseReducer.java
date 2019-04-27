/**
 * @author Karan Shah
 *
 */

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class HouseReducer extends Reducer<Text, Text, Text, Text> {
    public void reduce(Text airport, Iterable<Text> region, Context c) {
        int totalFlights = 0, inSum = 0, outSum = 0, countNAIn = 0, countNAOut = 0;

        int region1 = 0, region2 = 0, region3 = 0, region4 = 0, income1 = 0, income2 = 0, income3 = 0, income4 = 0;
        Iterator<Text> iter = region.iterator();

        while(iter.hasNext()){

            // region1
            System.out.println("region: "+region );
            inSum += Integer.parseInt(iter.toString());

        }
        //Format: OriginAirport, AvgTaxiInTime, AvgTaxiOutTime, countNATaxiIn, countNATaxiOut
        try {
            c.write(airport, new Text((inSum / totalFlights) + "\t" + (outSum / totalFlights) + "\t"
                    + countNAIn + "\t" + countNAOut) );
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }
}
