
import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.*;

import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import org.apache.hadoop.util.Tool;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount extends Configured implements Tool {

    public static Path players;
    public static Path teams;
    public static Path output1;
    public static Path lineitem;
    public static Path output2;
    public static Path product;
    public static Path output3;
    public static Path output4;
    public static Path output5;

    //FIRST JOB
    public static class PlayerMapper
            extends Mapper<LongWritable, Text, PairOfStrings, PairOfStrings> {

        public void map(LongWritable key, Text value, Context context
        ) throws IOException, InterruptedException {
            System.out.println("player mapper");
            if (key.get() == 0) {
                return;
            }

            String[] tokens = StringUtils.split(value.toString(), ",");

            String name = tokens[0].trim();
            String age = tokens[2].trim();
            String team = tokens[3].trim();
            //not sure if this is what i think it is
            String threeScore = tokens[9].trim();

            PairOfStrings outputKey = new PairOfStrings();

            PairOfStrings outputValue = new PairOfStrings();

            outputKey.set(new Text(team), new Text("2"));

            outputValue.set(new Text(age), new Text(name + ", " + threeScore));

            context.write(outputKey, outputValue);

        }

    }

    public static class TeamMapper
            extends Mapper<LongWritable, Text, PairOfStrings, PairOfStrings> {

        @Override

        public void map(LongWritable key, Text value, Context context)
                throws java.io.IOException, InterruptedException {
            System.out.println("team mapper");
            if (key.get() == 0) {
                return;
            }

            String[] tokens = StringUtils.split(value.toString(), ",");

            String team = tokens[0].trim();
            String teamdata = tokens[1].trim() + ", " + tokens[2].trim() + ", " + tokens[3].trim();

            PairOfStrings outputKey = new PairOfStrings();

            PairOfStrings outputValue = new PairOfStrings();

            outputKey.set(new Text(team), new Text("1"));

            outputValue.set(new Text("data"), new Text(teamdata));

            context.write(outputKey, outputValue);

        }

    }

    public static class IntSumReducer
            extends Reducer<PairOfStrings, PairOfStrings, Text, Text> {

        private int n = 10; // default

        private SortedSet<Record> top = new TreeSet<>();

        public void reduce(PairOfStrings key, Iterable<PairOfStrings> values,
                Context context
        ) throws IOException, InterruptedException {
            String cityData = "";
            int flag = 0;
            int count = 0;
            Integer totalAge = 0;
            int threeShot = 0;

            for (PairOfStrings temp : values) {

                if (flag == 0) {
                    //System.out.println("here");
                    //System.out.println("key: " + key.toString() + " value: " + temp.toString());

                    if (!temp.getLeftElement().toString().trim().equals("data")) {
                        return;
                    } else {
                        cityData = key.getLeftElement().toString() + ", " + temp.getRightElement().toString();
                    }
                    flag = 1;
                } else {
                    count++;
                    String[] tokens = StringUtils.split(temp.toString(), ",");

                    //System.out.println("age:" + tokens[0].trim());
                    //System.out.println("threeshot:" + tokens[2].trim());
                    //System.out.println("key: " + key.toString() + " value: " + temp.toString());
                    totalAge += Integer.parseInt(tokens[0].trim());
                    threeShot += Integer.parseInt(tokens[2].trim());

                }
                System.out.println("key: " + key.toString() + " value: " + temp.toString());

            }
            if (count <= 1) {
                return;
            }
            //System.out.println("about to write");
            System.out.println("count:" + count + " threeSHot:" + threeShot + " totalAge:" + totalAge);
            Double avgThree = (double) threeShot / (double) count;
            //System.out.println("calc avge");

            context.write(new Text(cityData + ","), new Text(totalAge.toString() + ", " + avgThree.toString()));
            System.out.println("end reduce");
        }
    }

    //SECOND JOB
    public static class Output1Mapper
            extends Mapper<Object, Text, PairOfStrings, PairOfStrings> {

        TreeMap<Integer, String> treeMap = new TreeMap<Integer, String>();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {

            System.out.println("output1Mapper");
            String[] tokens = StringUtils.split(value.toString(), ",");

            String teamData = tokens[0].trim() + ", " + tokens[1].trim() + ", " + tokens[2].trim() + ", " + tokens[3].trim();
            String age = tokens[4].trim();
            String threeAvg = tokens[5].trim();

            treeMap.put(Integer.parseInt(age), teamData + ", " + threeAvg);

        }

        @Override

        protected void cleanup(Context context) throws IOException,
                InterruptedException {
            System.out.println("CLEANUP");
            for (Map.Entry<Integer, String> temp : treeMap.entrySet()) {
                PairOfStrings outputKey = new PairOfStrings();

                PairOfStrings outputValue = new PairOfStrings();

                outputKey.set(new Text("1"), new Text("1"));

                outputValue.set(new Text(temp.getValue()), new Text(temp.getKey().toString()));
                context.write(outputKey, outputValue);

            }

        }

    }

    /*
    public static class LineItemMapper
            extends Mapper<Object, Text, PairOfStrings, PairOfStrings> {

        @Override

        public void map(Object key, Text value, Context context)
                throws java.io.IOException, InterruptedException {
            System.out.println("lineitemMapper");
            String[] tokens = StringUtils.split(value.toString(), ",");

            String sales_id = tokens[2];
            String product_id = tokens[3];
            String quantity = tokens[4];

            PairOfStrings outputKey = new PairOfStrings();

            PairOfStrings outputValue = new PairOfStrings();

            outputKey.set(new Text(sales_id), new Text("z"));

            outputValue.set(new Text(product_id), new Text(quantity));

            context.write(outputKey, outputValue);

        }

    }
     */
    public static class Output2Reducer
            extends Reducer<PairOfStrings, PairOfStrings, Text, Text> {

        private int n = 10; // default

        private SortedSet<Record> top = new TreeSet<>();

        public void reduce(PairOfStrings key, Iterable<PairOfStrings> values,
                Context context
        ) throws IOException, InterruptedException {

            System.out.println("REDUCE");
            String loc = "";
            int flag = 1;
            for (PairOfStrings temp : values) {

                context.write(new Text(""), new Text(temp.toString()));

            }

        }
    }

    //THIRD JOB
    public static class Output2Mapper
            extends Mapper<Object, Text, PairOfStrings, PairOfStrings> {

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {

            System.out.println("output3Mapper");
            String[] tokens = StringUtils.split(value.toString(), ",");

            String store_data = tokens[2].trim() + "," + tokens[3].trim();
            String date = tokens[4].trim();
            String product_id = tokens[5].trim();
            String quantity = tokens[6].trim();

            PairOfStrings outputKey = new PairOfStrings();

            PairOfStrings outputValue = new PairOfStrings();

            outputKey.set(new Text(product_id), new Text("z"));

            outputValue.set(new Text(store_data + "," + date), new Text(quantity));

            context.write(outputKey, outputValue);

        }

    }

    public static class ProductMapper
            extends Mapper<Object, Text, PairOfStrings, PairOfStrings> {

        @Override

        public void map(Object key, Text value, Context context)
                throws java.io.IOException, InterruptedException {
            System.out.println("productmapper");
            String[] tokens = StringUtils.split(value.toString(), ",");

            String product_id = tokens[1].trim();
            String price = tokens[3].trim().replace("$", "");

            PairOfStrings outputKey = new PairOfStrings();

            PairOfStrings outputValue = new PairOfStrings();

            outputKey.set(new Text(product_id), new Text("a"));

            outputValue.set(new Text(price), new Text(""));

            context.write(outputKey, outputValue);

        }

    }

    public static class Output3Reducer
            extends Reducer<PairOfStrings, PairOfStrings, Text, Text> {

        private int n = 10; // default

        private SortedSet<Record> top = new TreeSet<>();

        public void reduce(PairOfStrings key, Iterable<PairOfStrings> values,
                Context context
        ) throws IOException, InterruptedException {

            System.out.println("REDUCE3");
            String price = "";
            int flag = 1;
            for (PairOfStrings temp : values) {
                if (flag == 1) {
                    flag = 0;
                    price = temp.getLeftElement().toString();
                } else {
                    String quant = temp.getRightElement().toString();
                    //System.out.println("here");
                    //System.out.println(price.trim() + " " + quant.trim());
                    Double sales = Double.parseDouble(price.trim()) + Double.parseDouble(quant.trim());
                    //System.out.println("fuk");
                    context.write(temp.getLeftElement(), new Text("," + sales.toString()));
                }

                //System.out.println(key.toString() + " : " + temp.toString());
            }

        }
    }

    //fourth job
    public static class Output3Mapper
            extends Mapper<Object, Text, PairOfStrings, PairOfStrings> {

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {

            System.out.println("output3Mapper");
            String[] tokens = StringUtils.split(value.toString(), ",");

            String store_data = tokens[0].trim() + "," + tokens[1].trim();
            String date = tokens[2].trim();
            String sales = tokens[3].trim();

            PairOfStrings outputKey = new PairOfStrings();

            PairOfStrings outputValue = new PairOfStrings();

            outputKey.set(new Text(store_data + "," + date), new Text(""));

            outputValue.set(new Text(sales), new Text(""));

            context.write(outputKey, outputValue);

        }

    }

    public static class Output4Reducer
            extends Reducer<PairOfStrings, PairOfStrings, Text, Text> {

        private int n = 10; // default

        private TreeMap<String, Double> top = new TreeMap<String, Double>();

        private Text wat;

        public void reduce(PairOfStrings key, Iterable<PairOfStrings> values,
                Context context
        ) throws IOException, InterruptedException {

            System.out.println("REDUCE4");
            double price = 0;
            int flag = 1;
            for (PairOfStrings temp : values) {

                price += Double.parseDouble(temp.getLeftElement().toString());

            }
            System.out.println("end reduce");
            context.write(new Text(key.toString()), new Text(Double.toString(price)));
            wat = key.getLeftElement();

        }

    }

    //job 5
    public static class Output4Mapper
            extends Mapper<Object, Text, PairOfStrings, PairOfStrings> {

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {

            System.out.println("output3Mapper");
            String[] tokens = StringUtils.split(value.toString(), ",");

            String store_data = tokens[0].trim() + "," + tokens[1].trim();
            String date = tokens[2].trim();
            String sales = tokens[3].trim();

            PairOfStrings outputKey = new PairOfStrings();

            PairOfStrings outputValue = new PairOfStrings();

            outputKey.set(new Text(date), new Text(sales));

            outputValue.set(new Text(store_data), new Text(""));

            context.write(outputKey, outputValue);

        }

    }

    public static class Output5Reducer
            extends Reducer<PairOfStrings, PairOfStrings, Text, Text> {

        private int n = 10; // default

        private TreeMap<String, Double> top = new TreeMap<String, Double>();

        private Text wat;

        public void reduce(PairOfStrings key, Iterable<PairOfStrings> values,
                Context context
        ) throws IOException, InterruptedException {

            System.out.println("REDUCE5");

            for (PairOfStrings temp : values) {

                System.out.println(key.toString() + " : " + temp.toString());
                context.write(new Text(key.toString()), new Text(temp.toString()));
            }
            System.out.println("end reduce");
            //context.write(new Text(key.toString()), new Text(Double.toString(price)));

        }

    }

    public static class SecondarySortPartitioner extends Partitioner<PairOfStrings, PairOfStrings> {

        @Override
        public int getPartition(PairOfStrings key,
                PairOfStrings value,
                int numberOfPartitions) {

            return Math.abs(key.getLeftElement().hashCode())
                    % numberOfPartitions;

        }
    }

    public static class SecondarySortGroupingComparator extends WritableComparator {

        public SecondarySortGroupingComparator() {
            super(PairOfStrings.class, true);
        }

        @Override
        public int compare(WritableComparable wc1, WritableComparable wc2) {
            PairOfStrings pair = (PairOfStrings) wc1;
            PairOfStrings pair2 = (PairOfStrings) wc2;
            return pair.getLeftElement().compareTo(pair2.getLeftElement());
        }
    }

    public static void main(String[] args) throws Exception {
        FileUtils.deleteDirectory(new File("output1"));
        FileUtils.deleteDirectory(new File("output2"));
        FileUtils.deleteDirectory(new File("output3"));
        FileUtils.deleteDirectory(new File("output4"));
        FileUtils.deleteDirectory(new File("output5"));

        players = new Path(args[0]);
        teams = new Path(args[1]);
        output1 = new Path(args[2]);

        //lineitem = new Path(args[3]);
        output2 = new Path(args[3]);
        /*product = new Path(args[5]);
        output3 = new Path(args[6]);
        output4 = new Path(args[7]);
        output5 = new Path(args[8]);
         */
        System.exit((runJob1() && runJob2() /*&& runJob3() && runJob4() && runJob5()*/) ? 0 : 1);
        /*
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(WordCount.class);

        job.setPartitionerClass(SecondarySortPartitioner.class);
        job.setGroupingComparatorClass(SecondarySortGroupingComparator.class);
        //job.setSortComparatorClass(SecondarySortingComparator.class);
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, StoreMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, LeftJoinTransactionMapper.class);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        //job.setMapperClass(StoreMapper.class);
        job.setMapOutputKeyClass(PairOfStrings.class);
        job.setMapOutputValueClass(PairOfStrings.class);

        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        System.out.println("starting");
        boolean status = job.waitForCompletion(true);
         */
    }

    public int run(String[] args) throws Exception {

        return 0;
    }

    public static boolean runJob1() throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(WordCount.class);

        job.setPartitionerClass(SecondarySortPartitioner.class);
        job.setGroupingComparatorClass(SecondarySortGroupingComparator.class);
        //job.setSortComparatorClass(SecondarySortingComparator.class);
        MultipleInputs.addInputPath(job, players, TextInputFormat.class, PlayerMapper.class);
        MultipleInputs.addInputPath(job, teams, TextInputFormat.class, TeamMapper.class);
        FileOutputFormat.setOutputPath(job, output1);

        //job.setMapperClass(StoreMapper.class);
        job.setMapOutputKeyClass(PairOfStrings.class);
        job.setMapOutputValueClass(PairOfStrings.class);

        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        System.out.println("starting");
        boolean status = job.waitForCompletion(true);
        return status;
    }

    public static boolean runJob2() throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(WordCount.class);

        job.setPartitionerClass(SecondarySortPartitioner.class);
        job.setGroupingComparatorClass(SecondarySortGroupingComparator.class);
        //job.setSortComparatorClass(SecondarySortingComparator.class);
        MultipleInputs.addInputPath(job, output1, TextInputFormat.class, Output1Mapper.class);
        //MultipleInputs.addInputPath(job, lineitem, TextInputFormat.class, LineItemMapper.class);
        FileOutputFormat.setOutputPath(job, output2);

        //job.setMapperClass(StoreMapper.class);
        job.setMapOutputKeyClass(PairOfStrings.class);
        job.setMapOutputValueClass(PairOfStrings.class);

        job.setReducerClass(Output2Reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        System.out.println("starting");
        boolean status = job.waitForCompletion(true);
        return status;
    }

    public static boolean runJob3() throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(WordCount.class);

        job.setPartitionerClass(SecondarySortPartitioner.class);
        job.setGroupingComparatorClass(SecondarySortGroupingComparator.class);
        //job.setSortComparatorClass(SecondarySortingComparator.class);
        MultipleInputs.addInputPath(job, output2, TextInputFormat.class, Output2Mapper.class);
        MultipleInputs.addInputPath(job, product, TextInputFormat.class, ProductMapper.class);
        FileOutputFormat.setOutputPath(job, output3);

        //job.setMapperClass(StoreMapper.class);
        job.setMapOutputKeyClass(PairOfStrings.class);
        job.setMapOutputValueClass(PairOfStrings.class);

        job.setReducerClass(Output3Reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        System.out.println("starting");
        boolean status = job.waitForCompletion(true);
        return status;
    }

    public static boolean runJob4() throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(WordCount.class);

        job.setPartitionerClass(SecondarySortPartitioner.class);
        job.setGroupingComparatorClass(SecondarySortGroupingComparator.class);
        //job.setSortComparatorClass(SecondarySortingComparator.class);
        MultipleInputs.addInputPath(job, output3, TextInputFormat.class, Output3Mapper.class);
        //MultipleInputs.addInputPath(job, product, TextInputFormat.class, ProductMapper.class);
        FileOutputFormat.setOutputPath(job, output4);

        //job.setMapperClass(StoreMapper.class);
        job.setMapOutputKeyClass(PairOfStrings.class);
        job.setMapOutputValueClass(PairOfStrings.class);

        job.setReducerClass(Output4Reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        System.out.println("starting");
        boolean status = job.waitForCompletion(true);
        return status;
    }

    public static boolean runJob5() throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(WordCount.class);

        job.setPartitionerClass(SecondarySortPartitioner.class);
        job.setGroupingComparatorClass(SecondarySortGroupingComparator.class);
        //job.setSortComparatorClass(SecondarySortingComparator.class);
        MultipleInputs.addInputPath(job, output4, TextInputFormat.class, Output4Mapper.class);
        //MultipleInputs.addInputPath(job, product, TextInputFormat.class, ProductMapper.class);
        FileOutputFormat.setOutputPath(job, output5);

        //job.setMapperClass(StoreMapper.class);
        job.setMapOutputKeyClass(PairOfStrings.class);
        job.setMapOutputValueClass(PairOfStrings.class);

        job.setReducerClass(Output5Reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        System.out.println("starting");
        boolean status = job.waitForCompletion(true);
        return status;
    }
}
