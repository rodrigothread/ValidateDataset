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

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
//get ip and mac address
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;

public class CountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    InetAddress ip;

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(LongWritable Key, Text value, Context context)
            throws IOException, InterruptedException,SocketException,UnknownHostException {

        //ip = InetAddress.getLocalHost();
       // ip.getHostName();
        //System.out.println("Current IP address : " + ip.getHostAddress());

        //NetworkInterface network = NetworkInterface.getByInetAddress(ip);

        //byte[] mac = network.getHardwareAddress();

        //System.out.print("Current MAC address : ");
        //StringBuilder sb = new StringBuilder();
        //for (int i = 0; i < mac.length; i++) {
           // sb.append(String.format("%02X%s", mac[i], (i < mac.length - 1) ? "-" : ""));
        //}
        //System.out.println(sb.toString());

        String line = value.toString();
        StringTokenizer itr = new StringTokenizer(line);
        
        while (itr.hasMoreTokens()) {
            
            word.set("rodrigo");
            context.write(word, one);

        }

    }

}
