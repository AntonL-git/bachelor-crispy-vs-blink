package org.example;

import org.apache.hadoop.io.LongWritable;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import org.apache.spark.sql.SparkSession;

import org.apache.mahout.math.VectorWritable;


public class MLSampler {

    private static final String CONFIG_HIBENCH_FILE_PATH = "/Users/antonludchik/Documents/TUBerlin/HiBench/conf/hibench.conf";

    private static final String PROPERTY_KEY_INPUT = "hibench.workload.dir.name.input";
    private static final String PROPERTY_KEY_WORKLOAD_INPUT = "hibench.workload.input";

    private static final String PROPERTY_SAMPLED_VALUE = "Sampled";
    private static final String HDFS_URI = "hdfs://localhost:9000";

    //Change configuration parameters up to the payload
    private static final String HIBENCH_INPUT_PATH_WORDCOUNT = "/user/anton/HiBench/Wordcount/Input";
    private static final String HIBENCH_INPUT_PATH_ALS = "/user/anton/HiBench/ALS/Input";
    private static final String HIBENCH_INPUT_PATH_KMEANS = "/user/anton/HiBench/Kmeans/Input/samples";
    private static final String HIBENCH_INPUT_PATH_BAYES = "/user/anton/HiBench/Bayes/Input.parquet";
    private static final String HIBENCH_INPUT_PATH_SVM = "/user/anton/HiBench/SVM/Input";
    private static final String HIBENCH_INPUT_PATH_LR = "/user/anton/HiBench/LR/Input";

    private static final String HIBENCH_SAMPLED_PATH_WORDCOUNT = "/user/anton/HiBench/Wordcount/Sampled";
    private static final String HIBENCH_SAMPLED_PATH_ALS = "/user/anton/HiBench/ALS/Sampled";
    private static final String HIBENCH_SAMPLED_PATH_Kmeans = "/user/anton/HiBench/Kmeans/Sampled";
    private static final String HIBENCH_SAMPLED_PATH_Bayes = "/user/anton/HiBench/Bayes/Sampled";
    private static final String HIBENCH_SAMPLED_PATH_LR = "/user/anton/HiBench/LR/Sampled";
    private static final String HIBENCH_SAMPLED_PATH_SVM = "/user/anton/HiBench/SVM/Sampled";


    private static final String CONFIG_WORDCOUNT_FILE_PATH = "/Users/antonludchik/Documents/TUBerlin/HiBench/conf/workloads/micro/wordcount.conf";
    private static final String CONFIG_ALS_FILE_PATH = "/Users/antonludchik/Documents/TUBerlin/HiBench/conf/workloads/ml/als.conf";
    private static final String CONFIG_Kmeans_FILE_PATH = "/Users/antonludchik/Documents/TUBerlin/HiBench/conf/workloads/ml/kmeans.conf";
    private static final String CONFIG_Bayes_FILE_PATH = "/Users/antonludchik/Documents/TUBerlin/HiBench/conf/workloads/ml/bayes.conf";
    private static final String CONFIG_SVM_FILE_PATH = "/Users/antonludchik/Documents/TUBerlin/HiBench/conf/workloads/ml/svm.conf";
    private static final String CONFIG_LR_FILE_PATH = "/Users/antonludchik/Documents/TUBerlin/HiBench/conf/workloads/ml/lr.conf";



    /*
    * fraction - the fraction of the original dataset
    * sampleNum - the postfix in the generated name
    */
    public static void runSampling(double fraction, int sampleNum) {

        SparkContext context = new SparkContext(new SparkConf().setAppName("SimpleApp").setMaster("local[*]")
                .set("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000")
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer") // Use Kryo serializer
                .set("spark.kryo.registrator", org.example.MyKreoRegistrator.class.getName())
                .set("spark.kryo.registrationRequired", "true") // Optional: to ensure that all classes are registered
                .registerKryoClasses(new Class[]{
                        org.apache.mahout.math.VectorWritable.class,
                        org.apache.mahout.math.Vector.class,
                        org.apache.mahout.math.RandomAccessSparseVector.class
                })
                .set("spark.hadoop.fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName())
                .set("spark.hadoop.fs.hdfs.server", org.apache.hadoop.hdfs.server.namenode.NameNode.class.getName())
                .set("spark.hadoop.conf", org.apache.hadoop.hdfs.HdfsConfiguration.class.getName()));

        SparkSession spark = SparkSession.builder().sparkContext(context).getOrCreate();
        JavaSparkContext javaSparkContext = JavaSparkContext.fromSparkContext(spark.sparkContext());

        //Edit config_file
        SimpleApp.setInputPath(CONFIG_HIBENCH_FILE_PATH, PROPERTY_KEY_INPUT, PROPERTY_SAMPLED_VALUE + sampleNum);

        //Sampler for KMeans
        JavaPairRDD<LongWritable, VectorWritable> inputData = javaSparkContext.sequenceFile(
                HDFS_URI + HIBENCH_INPUT_PATH_KMEANS,
                LongWritable.class,
                VectorWritable.class
        );

        JavaRDD<VectorWritable> vectorData = inputData.values();

        JavaRDD<VectorWritable> sampledData = vectorData.sample(false, fraction);

        JavaRDD<VectorWritable> singlePartitionData = sampledData.coalesce(1);
        singlePartitionData.saveAsObjectFile(HDFS_URI + HIBENCH_SAMPLED_PATH_Kmeans + fraction + "CR");

        System.out.println("Sampled data with " + fraction + " fraction.");
        SimpleApp.setInputPath(CONFIG_SVM_FILE_PATH, PROPERTY_KEY_WORKLOAD_INPUT, "${hibench.hdfs.data.dir}/SVM/Sampled"+fraction+"CR");


        spark.stop();
    }


}
