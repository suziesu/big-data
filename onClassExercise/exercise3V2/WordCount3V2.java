/**
* @author Xi Su
* @version 2.0
* Bigdata Assignment-WordCount top N Frequency
* Not using HashMap and TreeMap in map side, 
* Using Hashmap treeMap which  I thought to reduce the data transmission
* but it turns out not this case
* set reduce task to be 1 is for the output to be 1 so that get one top 100 words output
*/

import java.io.IOException;
import java.util.*;
import java.util.Map;
import java.lang.*;
import java.util.TreeMap;
        
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
//asignment 3 version 2   
public class WordCount3V2 {


 public static class Mapp extends Mapper<LongWritable, Text, Text, IntWritable> {

    // private final static int MAX = 100;
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    // private TreeMap<Integer, String> treeMap = new TreeMap<Integer,String>();
    // private Map<String,Integer> hashmap = new HashMap<String,Integer>();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        StringTokenizer tokenizer = new StringTokenizer(line);

        while (tokenizer.hasMoreTokens()) {
            String token = tokenizer.nextToken();
            token =cleanToken(token);
            if(isAlphaWithSeven(token)){
                word.set(token);
                //put the token into map
                // int count = hashmap.containsKey(token)? hashmap.get(token):0;
                // hashmap.put(token,count + 1);
                context.write(word, one);

            }
        }

        /**
        * Before, try to use map and treeMap to sort top 100 word in each map site to 
        * reduce the bandwidth of data transmit from map to reduce
        * while, it turned out using hashmap, treemap in map site did not reduce time
        * It cause almost 20 min to complete the job.
        */
        //put hashmap <key value> into TreeMap
        // for(Map.Entry<String,Integer> entry : hashmap.entrySet()){
        //     treeMap.put(entry.getValue(),entry.getKey());
        //     if(treeMap.size() > MAX){
        //     treeMap.remove(treeMap.firstKey());
        //     }
        // }
        // for (Map.Entry<Integer, String> entry : treeMap.entrySet()){
        //     word.set(entry.getValue());
        //     context.write(word, new IntWritable(entry.getKey()));
        // }

    }
    /**
    * Before, try to use combiner to combine the value before reduce
    * then give up to using map instead, even though hashmap tree map is not a good way
    */
 //    public static class MyCombiner extends reducer<Text,IntWritable,Text,IntWritable>{
 //    @override
 //    public void reduce(Text key, Iterable<IntWritable> values, Context context) 
 //        throws IOException, InterruptedException{
 //            int sum = 0;
 //            for (IntWritable value: values){
 //                sum += value.get();
 //            }
 //            context.write(key, new IntWritable(sum));
 //    }
 // }
    /**
    * Before, try to use 2 mapreduce,
    * one is for doing the word count
    * another is just sort the count value by putting value as key using map site autosort
    * But not familiar with how 2 mapreduce combine, so give this up.
    */
    // public static class Map2 extends Mapper<LongWritable, Text, IntWritable, Text>{
    //     private Text word = new Text();
        
    //     public void map(LongWritable key, Text value, Context context) throws IOException,InterruptedException {
    //         String line = value.toString();
    //         String key = line.split(" ")[0];
    //         String value = line.split(" ")[1];
    //         int valueInt = Integer.parseInt(value.trim());
    //         word.set(key.trim());
    //         Context.write(new IntWritable(valueInt),word);
    //     }
    // }

    /**
    * check if token is length of 7
    */
    public boolean isAlphaWithSeven(String token){
        return token.length() == 7 ;
    }

    /**
    * clean the token 
    * get rid of those characters like ; , . ""
    * and also change chars to lowercase to calculate more presise.
    */
    public String cleanToken(String token){

        String cleanToken = token.replaceAll("[;:,.\"()?'-]","");
        return cleanToken.toLowerCase();
        // simply get rid of characters like "",();.
    }
 } 
 
    /**
    * instead of output reduce result to output,
    * using treeMap to get top 100 frequency result.
    * then using cleanup method to ouput results.
    */    
 public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
    private final static int MAX = 100;
    private TreeMap<Integer, String> reduceTreeMap = new TreeMap<Integer, String>();

    public void reduce(Text key, Iterable<IntWritable> values, Context context) 
      throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }
        reduceTreeMap.put(sum, key.toString());
        if (reduceTreeMap.size() > MAX){
            reduceTreeMap.remove(reduceTreeMap.firstKey());
        }
        // if(reduceTreeMap.containsValue(key.toString())){
        //     context.write(key, new IntWritable(sum));
        // }
    }
   /**
    * For Reduce cleanup process, output file after reduce finish
   */
    @Override
    protected void cleanup(Context context) throws IOException,InterruptedException{
        Text key = new Text();
        for(Map.Entry<Integer, String> entry : reduceTreeMap.entrySet()){
            key.set(entry.getValue());
            context.write(key, new IntWritable (entry.getKey()));
        }
    }

 }

 // public static  class Reduce2 extends Reducer<IntWritable, Text, Text, IntWritable>{
    
 //    public void reduce(IntWritable key, Iterable<Text> values, Context context)
 //    throws IOException, InterruptedException{
 //        if (count < MAX){
 //            for(Text word : values){
 //                context.write(word, key);
 //                count++;
 //            }
 //        }
        
 //    }
 // } 
 public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
        
        Job job = new Job(conf, "wordcount");
    
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
        
    job.setMapperClass(Mapp.class);
    // job.setCombinerClass(MyCombiner.class);
    job.setReducerClass(Reduce.class);
        
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    /**
    * not commenting this is that in this case we ONLY need to ouput one file with top 100
    * if this set to 2-3 or comment this, it will give two other wrong output.
    */
    job.setNumReduceTasks(1);
    job.setJarByClass(WordCount3V2.class);
        
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
    job.waitForCompletion(true);
 }
        
}