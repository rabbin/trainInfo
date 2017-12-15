package Unility;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import java.io.File;

public class Compare {
    public static void main(String[] args) {
        Long begin = System.currentTimeMillis();

        SparkSession sparkSession = SparkSession.builder().master("local[*]").appName("Compare").getOrCreate();

        String SparkTrainInfo =         "C:\\Users\\rabbin\\Desktop\\spark\\rabbin\\file\\Spark\\";
        String MutiThreadingTrainInfo = "C:\\Users\\rabbin\\Desktop\\spark\\rabbin\\file\\MutiThreading\\";
        String empty =         "C:\\Users\\rabbin\\Desktop\\spark\\rabbin\\file\\empty.txt";

        JavaRDD<String> sparkTrainInfo=sparkSession.read().textFile(empty).javaRDD();
        JavaRDD<String> mutiThreadingTrainInfo= sparkSession.read().textFile(empty).javaRDD();
        System.out.println("Now is in the directory of MutiThreading :");
        for(String result:new File(SparkTrainInfo).list()){
            sparkTrainInfo = sparkTrainInfo.union(sparkSession.read().textFile(SparkTrainInfo+result).javaRDD()).distinct();
            System.out.println("\tAfter file["+result+"] unioned,the number of train information is "+sparkTrainInfo.count());
        }
        System.out.println("----------------------------------------");
        System.out.println("Now is in the directory of Spark :");
        for(String result:new File(MutiThreadingTrainInfo).list()){
            mutiThreadingTrainInfo=mutiThreadingTrainInfo.
                    union(sparkSession.read().textFile(MutiThreadingTrainInfo+result).javaRDD()).distinct();
            System.out.println("\tAfter file["+result+"] unioned,the number of train information is "+mutiThreadingTrainInfo.count());
        }

        System.out.println("----------------------------------------");
        System.out.println("The total number of train information is "+sparkTrainInfo.union(mutiThreadingTrainInfo).distinct().count());

        Long end = System.currentTimeMillis();
        Long time = (end-begin)/1000;
        System.out.println("----------------------------------------");

        System.out.println("The program has run "+time/60 +" mins "+(end-begin)%60+" seconds");

    }
}
