package org.example;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.util.HashSet;
import java.util.Set;

public class GraphSampler {
    private static final String CONFIG_HIBENCH_FILE_PATH = "/Users/antonludchik/Documents/TUBerlin/HiBench/conf/hibench.conf";

    private static final String PROPERTY_KEY_INPUT = "hibench.workload.dir.name.input";
    private static final String PROPERTY_KEY_WORKLOAD_INPUT = "hibench.workload.input";

    private static final String PROPERTY_SAMPLED_VALUE = "Sampled";
    private static final String HDFS_URI = "hdfs://localhost:9000";

    //Change up to payload
    private static final String HIBENCH_INPUT_PATH_Pagerank_edges = "/user/anton/HiBench/Pagerank/Input/edges";
    private static final String HIBENCH_INPUT_PATH_Pagerank_vertices = "/user/anton/HiBench/Pagerank/Input/vertices";

    private static final String HIBENCH_SAMPLED_PATH_Pagerank_edges = "/user/anton/HiBench/Pagerank/Sampled0.005/edges";
    private static final String HIBENCH_SAMPLED_PATH_Pagerank_vertices = "/user/anton/HiBench/Pagerank/Sampled0.005/vertices";


    private static final String CONFIG_Pagerank_FILE_PATH = "/Users/antonludchik/Documents/TUBerlin/HiBench/conf/workloads/graph/pagerank.conf";


    /*
     * fraction - the fraction of the original dataset
     * sampleNum - the postfix in the generated name
     */
    public static void runSampling(double fraction, int sampleNum){
        SparkContext context = new SparkContext(new SparkConf().setAppName("SimpleApp").setMaster("local[*]")
                .set("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000")
                .set("spark.hadoop.fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName())
                .set("spark.hadoop.fs.hdfs.server", org.apache.hadoop.hdfs.server.namenode.NameNode.class.getName())
                .set("spark.hadoop.conf", org.apache.hadoop.hdfs.HdfsConfiguration.class.getName()));

        SparkSession spark = SparkSession.builder().sparkContext(context).getOrCreate();
        JavaSparkContext javaSparkContext = JavaSparkContext.fromSparkContext(spark.sparkContext());

        //Edit config_file
        SimpleApp.setInputPath(CONFIG_HIBENCH_FILE_PATH, PROPERTY_KEY_INPUT, PROPERTY_SAMPLED_VALUE + sampleNum);

        JavaRDD<String> edges = javaSparkContext.textFile(HIBENCH_INPUT_PATH_Pagerank_edges);
        JavaRDD<String> sampledEdgesRDD = edges.sample(false, fraction, 123);

        // Step 2: Extract vertices from the sampled edges
        Set<String> sampledVertices = new HashSet<>(
                sampledEdgesRDD.flatMap(edge -> {
                    String[] nodes = edge.split("\t");
                    return java.util.Arrays.asList(nodes[0], nodes[1]).iterator();
                }).collect()
        );

        // Step 3: Load and filter vertices based on sampled edges
        JavaRDD<String> vertices = javaSparkContext.textFile(HIBENCH_INPUT_PATH_Pagerank_vertices);
        JavaRDD<String> sampledVerticesRDD = vertices.filter(vertex -> sampledVertices.contains(vertex.split("\t")[0]));

        // Save sampled vertices and edges
        sampledVerticesRDD.saveAsTextFile(HIBENCH_SAMPLED_PATH_Pagerank_vertices);
        sampledEdgesRDD.saveAsTextFile(HIBENCH_SAMPLED_PATH_Pagerank_edges);

        System.out.println("Sampled data with " + fraction + " fraction.");
        SimpleApp.setInputPath(CONFIG_Pagerank_FILE_PATH, PROPERTY_KEY_WORKLOAD_INPUT, "${hibench.hdfs.data.dir}/Pagerank/Sampled"+fraction);

        spark.stop();
    }

}
