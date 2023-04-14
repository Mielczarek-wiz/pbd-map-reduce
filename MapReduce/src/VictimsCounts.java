import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Objects;

public class VictimsCounts extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new VictimsCounts(), args);
        System.exit(res);
    }

    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf(), "VictimsCounts");
        job.setJarByClass(this.getClass());
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Sequence file output
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        // MAPPER, COMBINER, REDUCER Classes
        job.setMapperClass(VictimCountsMapper.class);
        job.setCombinerClass(VictimCountsCombiner.class);
        job.setReducerClass(VictimCountsReducer.class);

        // MAPPER Output Key-Value
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        // REDUCER Output Key-Value
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class VictimCountsMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

        private final String[] Street = new String[3];
        private String Zip;
        private final Long[] Count = new Long[6];
        private static final String[] Type = {
                "PEDESTRIANS,INJURED",
                "PEDESTRIANS,KILLED",
                "CYCLIST,INJURED",
                "CYCLIST,KILLED",
                "MOTORIST,INJURED",
                "MOTORIST,KILLED"};
        private final String[] Key = new String[3];

        public void map(LongWritable offset, Text lineText, Context context) {
            try {
                if (offset.get() != 0) {
                    boolean canIwork = true;
                    String line = lineText.toString();
                    String[] splited = line.split(",", -1);
                    int i = 0;
                    for (String word : splited) {
                        // Year test
                        if (i == 0) {
                            if(Integer.parseInt(word.substring(word.lastIndexOf('/') + 1, word.lastIndexOf('/') + 5)) <= 2012){
                                canIwork = false;
                                break;
                            }
                        }

                        // Zip code test
                        if (i == 2) {
                            if(!Objects.equals(word, "")){
                                Zip = word;
                            }
                            else{
                                canIwork = false;
                                break;
                            }
                        }

                        // Street Test
                        if (i >= 6 && i <= 8) {
                            Street[i-6] = word;
                        }

                        // [PEDESTRIANS Injured, PEDESTRIANS Killed, CYCLIST Injured, CYCLIST Killed, MOTORIST Injured, MOTORIST Killed]
                        if (i >= 11  && i <= 16){
                            Count[i-11] = Long.parseLong(word);
                        }

                        i++;
                    }

                    // Context write
                    if(canIwork) {
                        // For every street in data
                        for (String street : Street) {
                            // If street exists
                            if (!Objects.equals(street, "")) {
                                // Set street
                                Key[0] = street;

                                // Set zip code
                                Key[1] = Zip;

                                // For every special case in [PEDESTRIANS Injured, PEDESTRIANS Killed, CYCLIST Injured, CYCLIST Killed, MOTORIST Injured, MOTORIST Killed]
                                for (int j = 0; j < 6; j++) {
                                    Key[2] = Type[j];
                                    // Context write
                                    context.write(new Text(Key[0] + "," + Key[1] + "," + Key[2]), new LongWritable(Count[j]));
                                }
                            }
                        }
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
    public static class VictimCountsReducer extends Reducer<Text, LongWritable, Text, Text> {

        Long suma;

        @Override
        public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {

            suma = 0L;

            // Sum up all values for key
            for (LongWritable val : values) {
                suma += val.get();
            }

            // Context write result
            context.write(new Text(""), new Text(key + "," + suma.toString()));
        }
    }

    public static class VictimCountsCombiner extends Reducer<Text, LongWritable, Text, LongWritable> {

        Long suma;

        @Override
        public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {

            suma = 0L;

            // Sum up some values for key
            for (LongWritable val : values) {
                suma += val.get();
            }

            // Context write result
            context.write(key, new LongWritable(suma));
        }
    }

}