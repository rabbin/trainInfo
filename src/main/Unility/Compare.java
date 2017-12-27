package Unility;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import java.io.File;

public class Compare {
    public static void main(String[] args) {
        Long begin = System.currentTimeMillis();

        SparkSession sparkSession = SparkSession.builder().master("local[*]").appName("Compare").getOrCreate();

        String SparkTrainInfo =         "C:\\Users\\rabbin\\Desktop\\spark\\rabbin\\file\\Spark\\";
        String MutiThreadingTrainInfo = "C:\\Users\\rabbin\\Desktop\\spark\\rabbin\\file\\MutiThreading\\";
        String empty =         "C:\\Users\\rabbin\\Desktop\\spark\\rabbin\\file\\empty.txt";


        // TODO: 2017/12/20 会有好的初始化方法

        JavaRDD<String> sparkTrainInfo=new JavaSparkContext(sparkSession.sparkContext()).emptyRDD();
        JavaRDD<String> mutiThreadingTrainInfo= new JavaSparkContext(sparkSession.sparkContext()).emptyRDD();
        JavaRDD<String> sparkStations=new JavaSparkContext(sparkSession.sparkContext()).emptyRDD();
        JavaRDD<String> mutiThreadingStations= new JavaSparkContext(sparkSession.sparkContext()).emptyRDD();

        System.out.println("Now is in the directory of MutiThreading\\traininfo:");
        for(String result:new File(SparkTrainInfo+"traininfo").list()){
            sparkTrainInfo = sparkTrainInfo.union(sparkSession.read().textFile(SparkTrainInfo+"traininfo\\"+result).javaRDD()).distinct();
            System.out.println("\tAfter file["+result+"] unioned,the number of train information is "+sparkTrainInfo.count());
        }
        System.out.println("----------------------------------------");
        System.out.println("Now is in the directory of Spark\\tarininfo:");
        for(String result:new File(MutiThreadingTrainInfo+"traininfo").list()){
            mutiThreadingTrainInfo=mutiThreadingTrainInfo.
                    union(sparkSession.read().textFile(MutiThreadingTrainInfo+"traininfo\\"+result).javaRDD()).distinct();
            System.out.println("\tAfter file["+result+"] unioned,the number of train information is "+mutiThreadingTrainInfo.count());
        }
        System.out.println("----------------------------------------");
        System.out.println("########################################");
        System.out.println("----------------------------------------");

        System.out.println("Now is in the directory of MutiThreading\\stations:");
        for(String result:new File(SparkTrainInfo+"stations").list()){
            sparkStations = sparkStations.union(sparkSession.read().textFile(SparkTrainInfo+"stations\\"+result).javaRDD()).distinct();
            System.out.println("\tAfter file["+result+"] unioned,the number of stations is "+sparkStations.count());
        }
        System.out.println("----------------------------------------");
        System.out.println("Now is in the directory of Spark\\stations:");
        for(String result:new File(MutiThreadingTrainInfo+"stations").list()){
            mutiThreadingStations=mutiThreadingStations.
                    union(sparkSession.read().textFile(MutiThreadingTrainInfo+"stations\\"+result).javaRDD()).distinct();
            System.out.println("\tAfter file["+result+"] unioned,the number of stations is "+mutiThreadingStations.count());
        }
        System.out.println();
        System.out.println("----------------------------------------");
        System.out.println("The total number of train information is "+sparkTrainInfo.union(mutiThreadingTrainInfo).distinct().count());
        System.out.println("----------------------------------------");
        System.out.println("The total number of train stations is "+sparkStations.union(mutiThreadingStations).distinct().count());
        Long end = System.currentTimeMillis();
        Long time = (end-begin)/1000;
        System.out.println("----------------------------------------");
        System.out.println();
        System.out.println("The program has run "+time/60 +" mins "+(end-begin)%60+" seconds");

        System.out.println(sparkSession.read().textFile("C:\\Users\\rabbin\\Desktop\\spark\\rabbin\\file\\1.txt").javaRDD().subtract(sparkTrainInfo).collect());

    }
}
