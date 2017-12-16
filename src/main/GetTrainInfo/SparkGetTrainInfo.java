package GetTrainInfo;


import java.io.*;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;

public class SparkGetTrainInfo implements Serializable {
    public static BufferedWriter writer = null;
    private static int UrlNum = 0;
    private  String phantomjsPath=null;
    final String splitChar = ",";
    private String stationPairs=null;
    SparkGetTrainInfo(String phantomjsPath, String trainsInfo,String stationPairs){
        this.phantomjsPath = phantomjsPath;
        this.stationPairs= stationPairs;
        try {
            int id = new File(trainsInfo).listFiles().length + 1;
            this.writer = new BufferedWriter(new FileWriter(new File(trainsInfo + id + ".txt")));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public static void main(String[] agrs) {

        Long begin = System.currentTimeMillis();


        final String phantomjsPath = "C:\\Users\\rabbin\\Desktop\\spark\\rabbin\\conf\\";
        final String stationPairs = "C:\\Users\\rabbin\\Desktop\\spark\\rabbin\\file\\stationPairs.txt";
        final String trainsInfo = "C:\\Users\\rabbin\\Desktop\\spark\\rabbin\\file\\Spark\\";

        new SparkGetTrainInfo(phantomjsPath,trainsInfo,stationPairs).begin();
        Long end = System.currentTimeMillis();
        Long time = (end - begin) / 1000;
        System.out.println("The program has run " + time / 60 + " mins " + (end - begin) % 60 + " seconds");

    }


    /**
     * SparkGetTrainInfo
     */

    public void begin() {

        SparkSession sparkSession = SparkSession.builder().appName("getTrainInfo").master("local[*]").getOrCreate();

        JavaRDD<String> pair = sparkSession.read().textFile(stationPairs).javaRDD();


        pair.foreach(s -> {
            String[] stations = s.split(splitChar);
            //        String url = "http://trains.ctrip.com/TrainBooking/Search.aspx?from=beijing&to=shanghai&day=2";
            String url = "http://trains.ctrip.com/TrainBooking/Search.aspx?from=" + stations[1] + "&to=" + stations[3] + "&day=2";
            System.out.println(url);
            GetTrainInfo.getTrainInfo(SparkGetTrainInfo.writer, url, phantomjsPath);
            SparkGetTrainInfo.UrlNum++;
            System.out.println("****" + SparkGetTrainInfo.UrlNum + "*******");
        });

        try {
            SparkGetTrainInfo.writer.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
        //System.out.println(pair.UrlNum());
        // 7126230
        //pair.collect();
    }


}


