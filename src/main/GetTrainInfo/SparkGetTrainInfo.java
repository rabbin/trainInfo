package GetTrainInfo;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

public class SparkGetTrainInfo implements Serializable {
    public static void main(String[] agrs) {

        Long begin = System.currentTimeMillis();

        final String phantomjsPath = "C:\\Users\\rabbin\\Desktop\\spark\\rabbin\\conf\\";
        final String SparkTrain = "C:\\Users\\rabbin\\Desktop\\spark\\rabbin\\SparkTrain\\";
        int reset=0;
        new SparkGetTrainInfo(phantomjsPath, SparkTrain,reset).begin();
        Long end = System.currentTimeMillis();
        Long time = (end - begin) / 1000;
        System.out.println("The program has run " + time / 60 + " mins " + (end - begin) % 60 + " seconds");

    }

    private static int urlNum = 0;
    private final String splitChar = ",";
    private final int slices = 8;
    private String phantomjsPath = null;
    private final int flag = 1;
    private SparkSession sparkSession = null;

    private static BufferedWriter trainInfoWriter = null;
    private static BufferedWriter stationsWriter = null;
    private static BufferedWriter noStationsWriter = null;

    private Long  pairsNum;

    JavaRDD<String> stationsPairs = null;
    Map stationsCodeMap = null;
    int[][] stationsMatrix = null;


    SparkGetTrainInfo(String phantomjsPath, String SparkTrain,int reset) {


        this.sparkSession = SparkSession.builder().appName("getTrainInfo").master("local[*]").getOrCreate();
        this.phantomjsPath = phantomjsPath;

        this.stationsCodeMap = new HashMap();
        int count = 0;
        try {
            BufferedReader reader = new BufferedReader(new FileReader(new File(SparkTrain+"stations.txt")));
            String s;
            while ((s = reader.readLine()) != null) {
                this.stationsCodeMap.put(s.split(",")[1], count);
                count++;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        this.stationsMatrix = new int[count][count];

        for (int i = 0; i < count; i++) {
            for (int j = 0; j < count; j++) {
                this.stationsMatrix[i][j] = flag;
            }
        }

        List<String> stationsPairsList = new ArrayList<>();

        try {
            BufferedReader reader = new BufferedReader(new FileReader(new File(SparkTrain+"stationPairs.txt")));
            String line;
            while ((line = reader.readLine()) != null) {
                stationsPairsList.add(line);
            }
            reader.close();
        } catch (Exception e) {
            e.printStackTrace();

        }

        stationsPairs = new JavaSparkContext(sparkSession.sparkContext()).parallelize(stationsPairsList, slices);
        for (String result : new File(SparkTrain + "stations").list()) {
            JavaRDD<String> stations = sparkSession.read().textFile(SparkTrain + "stations\\" + result).javaRDD();
            stationsPairs = stationsPairs.subtract(stations);
        }           //for stations

      if(reset==0){
          for (String result : new File(SparkTrain + "noStations").list()) {
              JavaRDD<String> stations = sparkSession.read().textFile(SparkTrain + "noStations\\" + result).javaRDD();
              stations.foreach(line->{
                  String[] startAndArrive = line.split(",");
                  Tuple2<Integer,Integer> index=getIndex(startAndArrive[0],startAndArrive[1]);
                  stationsMatrix[index._1()][index._2()]=0;
                  stationsMatrix[index._2()][index._1()]=0;
              });
          }               //for noStations
      }

        try {
            int id = new File(SparkTrain + "traininfo\\").listFiles().length + 1;
            this.stationsWriter = new BufferedWriter(new FileWriter(new File(SparkTrain + "stations\\" + id + ".txt")));
            this.trainInfoWriter = new BufferedWriter(new FileWriter(new File(SparkTrain + "traininfo\\" + id + ".txt")));
            this.noStationsWriter=new BufferedWriter(new FileWriter(new File(SparkTrain+"noStations\\"+id+".txt")));

        } catch (Exception e) {
            e.printStackTrace();

        }
        pairsNum=stationsPairs.count();
    }


    /**
     * SparkGetTrainInfo
     */

    public void begin() {

        stationsPairs.foreach(s -> {
            String[] stations = s.split(splitChar);
            //        String url = "http://trains.ctrip.com/TrainBooking/Search.aspx?from=beijing&to=shanghai&day=2";
            String url = "http://trains.ctrip.com/TrainBooking/Search.aspx?from=" + stations[0] + "&to=" + stations[1] + "&day=2";

            Tuple2<Integer, Integer> index = getIndex(stations[0], stations[1]);
            if (stationsMatrix[index._1()][index._2()] == flag) {
                if (GetTrainInfo.getTrainInfo(trainInfoWriter, url, phantomjsPath)) {
                    stationsMatrix[index._2()][index._1()] = 1;
                    stationsWriter.write(s + "\n");
                    stationsWriter.flush();
                } else {
                    System.out.println("\n"+url+"\nThe two stations has no direct train!\n");
                    stationsMatrix[index._1()][index._2()] = 0;
                    stationsMatrix[index._2()][index._1()] = 0;
                    noStationsWriter.write(s+"\n");
                    noStationsWriter.flush();
                }
            } else if (stationsMatrix[index._1()][index._2()] == 0) {
                System.out.println("\n*****"+url+"\nThe two stations has no direct train!\n");
            }

            urlNum++;
            System.out.printf("--------->%.2f%%\n",(double)urlNum*100/pairsNum);

        });

        try {
            trainInfoWriter.close();
            stationsWriter.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private Tuple2<Integer, Integer> getIndex(String start, String arrive) {
        return new Tuple2<>((Integer) stationsCodeMap.get(start), (Integer) stationsCodeMap.get(arrive));
    }

}


