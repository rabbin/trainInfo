package GetTrainInfo;


import java.io.*;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

public class SparkGetTrainInfo implements Serializable {

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




    private static BufferedWriter TrainInfoWriter = null;
    private static BufferedWriter StationsExitsWriter = null;

    private static int UrlNum = 0;
    final String splitChar = ",";
    final int slices = 8;
    private  String phantomjsPath=null;
    private String stationPairs=null;
    SparkGetTrainInfo(String phantomjsPath, String trainsInfo,String stationPairs){
        this.phantomjsPath = phantomjsPath;
        this.stationPairs= stationPairs;
        try {
            int id = new File(trainsInfo+ "traininfo\\").listFiles().length + 1;
            this.StationsExitsWriter= new BufferedWriter(new FileWriter(new File(trainsInfo+"stations\\"+id+".txt")));
            this.TrainInfoWriter = new BufferedWriter(new FileWriter(new File(trainsInfo + "traininfo\\"+id + ".txt")));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }




    /**
     * SparkGetTrainInfo
     */

    public void begin() {

        SparkSession sparkSession = SparkSession.builder().appName("getTrainInfo").master("local[*]").getOrCreate();
        List<String> stationsPairsList = new ArrayList<>();

        try{
            BufferedReader reader = new  BufferedReader(new FileReader(new File(stationPairs)));
            String line;
            while((line= reader.readLine())!=null){
                stationsPairsList.add(line);
            }
            reader.close();
        }catch(Exception e){
            e.printStackTrace();
        }

        JavaRDD<String> pair = new JavaSparkContext(sparkSession.sparkContext()).parallelize(stationsPairsList,slices);


        pair.foreach(s -> {
            String[] stations = s.split(splitChar);
            //        String url = "http://trains.ctrip.com/TrainBooking/Search.aspx?from=beijing&to=shanghai&day=2";
            String url = "http://trains.ctrip.com/TrainBooking/Search.aspx?from=" + stations[1] + "&to=" + stations[3] + "&day=2";
            if(GetTrainInfo.getTrainInfo(TrainInfoWriter, url, phantomjsPath)){
                StationsExitsWriter.write(stations[1]+","+stations[3]+"\n");
                StationsExitsWriter.flush();
            }
            UrlNum++;
            System.out.println("****" + SparkGetTrainInfo.UrlNum + "*******");
        });

        try {
            TrainInfoWriter.close();
            StationsExitsWriter.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
        //System.out.println(pair.UrlNum());
        // 7126230
        //pair.collect();
    }


}


