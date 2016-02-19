/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package validatenacluster;

/**
 *
 * @author rodrigob
 */
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ValidateNacluster {
        String configurationHdfs;
    public static void main(String[] args) throws Exception {

        if (args.length != 2) {

            System.err.println("Usage: ValidateNacluster <input path/folder> <output path/folder>");
            System.exit(-1);
        }

        Job job = new Job();
        job.setJarByClass(ValidateNacluster.class);
        job.setJobName("Partition for Machine Count");
        /*String configurationHdfs = "";
        Configuration conf;
        conf = new Configuration();

        URI uri = new URI(configurationHdfs);

        FileSystem hdfs = FileSystem.get(uri, conf);
   
        // Delete old folder in the parameter two
        Path newFolderPath = new Path(args[1]);
       
        if (hdfs.exists(newFolderPath)) {
            hdfs.delete(newFolderPath, true); //Delete existing Directory
        }*/

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        job.setMapperClass(CountMapper.class);
        
        job.setReducerClass(CountReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
