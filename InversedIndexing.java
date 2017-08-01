import java.io.IOException;
import java.util.*;
import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;
import java.lang.*;
import java.util.ArrayList;
import java.util.Comparator;
import javafx.util.Pair;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class InversedIndexing 
{
    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text>
     {
	    private JobConf conf;
        public void configure(JobConf job)
        {
            this.conf = job;
        }

	    public void map(LongWritable docId, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException 
        {
	        int argc = Integer.parseInt( conf.get("argc") );
			//Hashmap to store keywords and manage counts
            HashMap<String, Integer> keywords = new HashMap<String, Integer>();
            for(int i = 0; i < argc; ++i)
            {
				//get  the keywords and set count to 0
                keywords.put(conf.get("keyword"+i), Integer.valueOf(0));
            }

            FileSplit fileSplit = (FileSplit)reporter.getInputSplit();
            String filename = "" + fileSplit.getPath().getName();
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);
			//check if it is one of the given keywords; and increment this keyword count.
            while (tokenizer.hasMoreTokens()) {
                String t = tokenizer.nextToken();
                if (keywords.containsKey(t))
                {
                    Integer num = keywords.get(t);
                    num = num+1;
                    keywords.put(t, num);
                }
            }
    //Once you read all the file splits, you will generate and pass pairs of a keyword and a document id to Reduce.
            Set<String> keys = keywords.keySet();
            for(String key: keys)
            {
                Integer num = keywords.get(key);
                if (num > 0)
                {
                    String sValue = filename + "_" + num.toString();
                    output.collect(new Text(key), new Text(sValue));
                }
            }
	    }   
    }
	 
    public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> 
    {
        public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException 
        {
            String sum = "";
			//prepare a hashmap to maintain all filename_counts
            HashMap<String, Integer> keywords = new HashMap<String, Integer>();
            while (values.hasNext())
            {
                String s = values.next().toString();

                String[] parts = s.split("[_]+");
                if (parts.length >= 2)
                {
                    String file = parts[0]; // 004
                    String part2 = parts[1]; // 034556

                    Integer num = Integer.parseInt(part2);
					//check if the same filename is already stored in the document container;
                    if (keywords.containsKey(file))
                    {
					//add up the count of the current filename_count to that found in the document container; 
                        Integer c = keywords.get(file);
                        c = c+num;
                        keywords.put(file, c);
                    }
					//if not,insert the current filename_count into the document container.
                    else
                    {
                        keywords.put(file, num);
                    }
                }
                else
                {
                    sum += s;
                }
            }

            
            if (!keywords.isEmpty())
            {
                sum = "";
                Set<String> keys = keywords.keySet();
                 for(String f: keys)
                 {
                     Integer c = keywords.get(f);
                     sum += " " + f + " " + c.toString();
                 }
                output.collect(key, new Text(sum));
            }
            else
            {			
                String[] parts = sum.trim().split("[ ]+");
				// sort the returned results in decreasing order
                ArrayList<Pair<Integer, String>> sortedArray = new ArrayList<Pair<Integer, String>>();
                for(int i = 0; i < parts.length; i+=2)
                {
                    Integer c = Integer.valueOf(parts[i+1]);
                    sortedArray.add(new Pair<Integer, String>(c, parts[i]));
                }

                //sort the ArrayList base on value of count.
                sortedArray.sort(new Comparator<Pair<Integer, String>>() {
                    @Override
                    public int compare(Pair<Integer, String> o1, Pair<Integer, String> o2) {
                        return o2.getKey().compareTo(o1.getKey());
                    }
                });

                sum = "";
                for(int i = 0; i < sortedArray.size(); ++i)
                {
                    sum += " " + sortedArray.get(i).getValue() + " " + sortedArray.get(i).getKey().toString();
                }
				//wrtie resluts to the final output
                output.collect(key, new Text(sum));
            }
        }
    }
    
    public static void main(String[] args) throws Exception 
    {
        JobConf conf = new JobConf(InvertedIndex.class);
        conf.setJobName("invertedindex");
        
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);
        
        conf.setMapperClass(Map.class);
        conf.setCombinerClass(Reduce.class);
        conf.setReducerClass(Reduce.class);
        
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);
        
        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));
            
        conf.set("argc", String.valueOf(args.length - 2));
        for (int i = 0; i < args.length - 2; ++i)
        {
            conf.set("keyword" + i, args[i+2]);
        }

        JobClient.runJob(conf);
    }
}
