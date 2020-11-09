import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.StringUtils;

public class Least5 {

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, LongWritable>{

        static enum CountersEnum { INPUT_WORDS }

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        private boolean caseSensitive;
        private Set<String> patternsToSkip = new HashSet<String>();

        private Configuration conf;
        private BufferedReader fis;

        private HashMap<String,Long> hmap;
        private TreeMap<Long, List<String>> tmap;
        @Override
        public void setup(Context context) throws IOException,
                InterruptedException {
            conf = context.getConfiguration();
            caseSensitive = conf.getBoolean("wordcount.case.sensitive", true);
            if (conf.getBoolean("wordcount.skip.patterns", false)) {
                URI[] patternsURIs = Job.getInstance(conf).getCacheFiles();
                for (URI patternsURI : patternsURIs) {
                    Path patternsPath = new Path(patternsURI.getPath());
                    String patternsFileName = patternsPath.getName().toString();
                    parseSkipFile(patternsFileName);
                }
            }
            hmap = new HashMap<String, Long>();
            tmap = new TreeMap<Long, List<String>>();
        }

        private void parseSkipFile(String fileName) {
            try {
                fis = new BufferedReader(new FileReader(fileName));
                String pattern = null;
                while ((pattern = fis.readLine()) != null) {
                    patternsToSkip.add(pattern);
                }
            } catch (IOException ioe) {
                System.err.println("Caught exception while parsing the cached file '"
                        + StringUtils.stringifyException(ioe));
            }
        }

        @Override
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String line = (caseSensitive) ?
                    value.toString() : value.toString().toLowerCase();
            for (String pattern : patternsToSkip) {
                line = line.replaceAll(pattern, "");
            }
            StringTokenizer itr = new StringTokenizer(line);
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                //context.write(word, one);
                if(hmap.get(String.valueOf(word)) == null) {
                    hmap.put(String.valueOf(word), new Long(1));
                }else {
                    hmap.put(String.valueOf(word), (hmap.get(String.valueOf(word))+1));
                }
            }
        }

        @Override
        public void cleanup(Context context)throws IOException,InterruptedException
        {
            System.out.println("====================HERE===============================" + hmap.size());
            for(Map.Entry<String,Long> entry : hmap.entrySet()) {

                String word = entry.getKey();
                Long count = entry.getValue();
                context.write(new Text(word),new LongWritable(count));
                //tmap.put(count, word);

//                if (tmap.size() > 5)
//                {
//                    tmap.remove(tmap.lastKey());
//                }

            }
//            for(Map.Entry<Long, String> entry : tmap.entrySet()){
//                Long count2 = entry.getKey();
//                String word2 = entry.getValue();
//                context.write(new Text(word2),new LongWritable(count2));
//            }
        }
    }

    public static class IntSumReducer
            extends Reducer<Text,LongWritable,Text,LongWritable> {
        private IntWritable result = new IntWritable();
        private TreeMap<Long, List<String>> tmap2;
        private long debug = 0;
        private int tsize = 0;

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            tmap2 = new TreeMap<Long, List<String>>();
        }

        public void reduce(Text key, Iterable<LongWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            String word = key.toString();
            long count = 0;

            for (LongWritable val : values)
            {
                count += val.get();
            }
            //tmap2.put(count, word);
            if(tmap2.get(count) == null){
                tmap2.put(count, new LinkedList<String>());
                tsize++;
            }
            tmap2.get(count).add(word);
           // context.write(new Text(word) , new LongWritable(count));

            // we remove the first key-value
            // if it's size increases 10
            if (tsize > 5)
            {
                tmap2.remove(tmap2.lastKey());
                tsize--;
            }
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            for (Map.Entry<Long, List<String>> entry : tmap2.entrySet()) {
                Long count = entry.getKey();
                LinkedList<String> words = (LinkedList<String>) entry.getValue();
                for(int i=0; i<words.size(); i++) {
                    context.write(new Text(words.get(i)), new LongWritable(count));
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        long startTime = System.currentTimeMillis();
        Configuration conf = new Configuration();
        GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
        String[] remainingArgs = optionParser.getRemainingArgs();
        if ((remainingArgs.length != 2) && (remainingArgs.length != 4)) {
            System.err.println("Usage: wordcount <in> <out> [-skip skipPatternFile]");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(Least5.class);
        job.setMapperClass(TokenizerMapper.class);
        //job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        List<String> otherArgs = new ArrayList<String>();
        for (int i=0; i < remainingArgs.length; ++i) {
            if ("-skip".equals(remainingArgs[i])) {
                job.addCacheFile(new Path(remainingArgs[++i]).toUri());
                job.getConfiguration().setBoolean("wordcount.skip.patterns", true);
            } else {
                otherArgs.add(remainingArgs[i]);
            }
        }
        job.setNumReduceTasks(1); //set the number of reducer to 1
        FileInputFormat.addInputPath(job, new Path(otherArgs.get(0)));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs.get(1)));

        if(job.waitForCompletion(true)){
            long estimatedTime = System.currentTimeMillis() - startTime;
            System.out.println("Execution time: " + estimatedTime);
            System.exit(0);
        }else{
            System.exit(1);
        }
        //›System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}