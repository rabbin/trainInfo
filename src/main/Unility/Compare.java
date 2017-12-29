package Unility;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import scala.Tuple3;

import java.io.File;

public class Compare {

    private static String SparkTrainInfo;
    private static String MultiThreadingTrainInfo;

    Compare(String SparkTrainInfo, String MultiThreadingTrainInfo) {
        this.SparkTrainInfo = SparkTrainInfo;
        this.MultiThreadingTrainInfo = MultiThreadingTrainInfo;
    }


    /**
     *
     */
    void begin() {
        SparkSession sparkSession = SparkSession.builder().master("local[*]").appName("Compare").getOrCreate();

        JavaRDD<String> sparkTrainInfo = new JavaSparkContext(sparkSession.sparkContext()).emptyRDD();
        JavaRDD<String> mutiThreadingTrainInfo = new JavaSparkContext(sparkSession.sparkContext()).emptyRDD();
        JavaRDD<String> sparkStations = new JavaSparkContext(sparkSession.sparkContext()).emptyRDD();
        JavaRDD<String> mutiThreadingStations = new JavaSparkContext(sparkSession.sparkContext()).emptyRDD();

        /**
         * 合并所有由spark获得的火车信息
         */
        System.out.println("Now is in the directory of Spark\\tarininfo:");
        PrintTitle(1);
        for (String result : new File(SparkTrainInfo + "traininfo").list()) {
            JavaRDD<String> train = sparkSession.read().textFile(SparkTrainInfo + "traininfo\\" + result).javaRDD();
            System.out.println("\t"+result + "\t" + train.count());
            sparkTrainInfo = sparkTrainInfo.union(train).distinct();
        }
        /**
         * 合并所有由multiThreading获得的火车信息
         */
        System.out.println("----------------------------------------");
        System.out.println("Now is in the directory of MutiThreading\\traininfo:");
        PrintTitle(1);
        for (String result : new File(MultiThreadingTrainInfo + "traininfo").list()) {
            JavaRDD<String> train = sparkSession.read().textFile(MultiThreadingTrainInfo + "traininfo\\" + result).javaRDD();
            System.out.println("\t"+result + "\t" + train.count());
            mutiThreadingTrainInfo = mutiThreadingTrainInfo.union(train).distinct();
        }
        System.out.println("----------------------------------------");
        System.out.println("########################################");
        System.out.println("----------------------------------------");
        /**
         * 合并由spark获得的有直达列车的车站信息
         */
        System.out.println("Now is in the directory of Spark\\stations:");
        PrintTitle(2);

        for (String result : new File(SparkTrainInfo + "stations").list()) {
            JavaRDD<String> stations = sparkSession.read().textFile(SparkTrainInfo + "stations\\" + result).javaRDD();
            System.out.println("\t"+result + "\t" + stations.count());
            sparkStations = sparkStations.union(stations).distinct();
        }
        System.out.println("----------------------------------------");
        /**
         * 合并由multiThreading获得的有直达列车的车站信息
         */
        System.out.println("Now is in the directory of MutiThreading\\stations:");
        PrintTitle(2);
        for (String result : new File(MultiThreadingTrainInfo + "stations").list()) {
            JavaRDD<String> stations=sparkSession.read().textFile(MultiThreadingTrainInfo + "stations\\" + result).javaRDD();
            System.out.println("\t"+result + "\t" + stations.count());
            mutiThreadingStations = mutiThreadingStations.union(stations).distinct();

        }

        /**
         * 输出结果
         */
        System.out.println();
        System.out.println("----------------------------------------");
        System.out.println("The total number of train information is " + sparkTrainInfo.union(mutiThreadingTrainInfo).distinct().count());
        System.out.println("----------------------------------------");
        System.out.println("The total number of train stations is " + sparkStations.union(mutiThreadingStations).distinct().count());


        JavaRDD<Tuple3<String, String, String>> a = sparkTrainInfo.map(line -> GetMainInfo(line));

    }

    /**
     *
     * @param flag
     */
    private void PrintTitle(int flag) {
        if (flag == 1) {
            System.out.println("\t"+"File\ttrains num");
        } else if (flag == 2) {
            System.out.println("\t"+"File\tstations num");
        }
    }

    /**
     *
     * @param line
     * @return
     */
    private static Tuple3<String, String, String> GetMainInfo(String line) {
        String[] info = line.split(",");
        return new Tuple3<String, String, String>(info[0], info[4], info[5]);
    }

    /**
     *
     * @param args
     */
    public static void main(String[] args) {
        Long begin = System.currentTimeMillis();
        String SparkTrainInfo = "C:\\Users\\rabbin\\Desktop\\spark\\rabbin\\file\\Spark\\";
        String MultiThreadingTrainInfo = "C:\\Users\\rabbin\\Desktop\\spark\\rabbin\\file\\MultiThreading\\";

        new Compare(SparkTrainInfo, MultiThreadingTrainInfo).begin();
        Long end = System.currentTimeMillis();
        Long time = (end - begin) / 1000;
        System.out.println("----------------------------------------");
        System.out.println();
        System.out.println("The program has run " + time / 60 + " mins " + (end - begin) % 60 + " seconds");
    }

}
