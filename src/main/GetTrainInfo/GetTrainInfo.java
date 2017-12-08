package GetTrainInfo;

import org.jsoup.*;
import org.jsoup.nodes.*;
import org.jsoup.select.Elements;

import java.io.*;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;

public class GetTrainInfo {

    public static final String phantomjsPath = "C:\\Users\\rabbin\\Desktop\\spark\\rabbin\\conf\\";
    public static final String stationPairs = "C:\\Users\\rabbin\\Desktop\\spark\\rabbin\\file\\stationPairs.txt";
    public static final String trainsInfo = "C:\\Users\\rabbin\\Desktop\\spark\\rabbin\\file\\trainsInfo.txt";
    public static BufferedWriter writer= null;
    public static int count = 0;
    public static final String splitChar =",";

    public static void main(String[] agrs) {

        SparkSession sparkSession = SparkSession.builder().appName("getTrainInfo").master("local[4]").getOrCreate();

        JavaRDD<String> pair = sparkSession.read().textFile(GetTrainInfo.stationPairs).javaRDD();

        try{
            //不能放在函数中
            GetTrainInfo.writer = new BufferedWriter(new FileWriter(new File(GetTrainInfo.trainsInfo)));

        }catch (IOException IOException){
            System.err.println(IOException);
            System.exit(1);
        }

        pair.foreach(s->{
            String[] stations = s.split(splitChar);
            //        String url = "http://trains.ctrip.com/TrainBooking/Search.aspx?from=beijing&to=shanghai&day=2";
            String url = "http://trains.ctrip.com/TrainBooking/Search.aspx?from="+stations[1]+"&to="+stations[3]+"&day=2";
            System.out.println(url);
            GetTrainInfo.getTrainInfo(url);
            GetTrainInfo.count++;
            System.out.println("****"+GetTrainInfo.count+"*******");
        });

        try{
            GetTrainInfo.writer.close();

        }catch (IOException IOException){
            System.err.println(IOException);
            System.exit(3);
        }
        //System.out.println(pair.count());
        // 7126230
        //pair.collect();

    }


    public static void getTrainInfo(String url) {

        try {
            Runtime rt = Runtime.getRuntime();
            Process phantomjs = rt.exec(phantomjsPath + "phantomjs.exe " + phantomjsPath + "code.js " + url);

            InputStream inputStream = phantomjs.getInputStream();
            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
            StringBuffer doc = new StringBuffer();
            String tmp ;
            while ((tmp = reader.readLine()) != null) {
                doc.append(tmp);
            }



            Elements tbody = Jsoup.parse(doc.toString(), "UTF-8").getElementsByClass("tbody");

            for (Element i : tbody) {

                String train = i.getElementsByClass("w1").text().split(" ")[0];
                String times[] = i.getElementsByClass("w2").text().split(" ");
                String stations[] = i.getElementsByClass("w3").text().split(" ");

                String startTime =times[0];
                String arriveTime = times[1];
                int day = times.length == 2 ? 0 : Integer.parseInt(times[2]);
                String startStation = stations[1];
                String arriveStation = stations[3];


                System.out.print("train: " + train + "\t\t"); //车次
                System.out.print("start time: " + startTime + "\t\t");
                System.out.print("arrive time: " + arriveTime+ "\t\t");
                System.out.print("+" + day + "\t");
                System.out.print("start station: " + startStation + "\t\t");
                System.out.println("arrive station: " + arriveStation);          //

                GetTrainInfo.writer.write(train+","+startTime+","+arriveTime+","+day+","+startStation+","+arriveStation+"\n");
                GetTrainInfo.writer.flush();
                //System.out.println(i.getElementsByClass("w4").text());          //运行时间，可以计算出来，没有必要保存

            }
        } catch (IOException IOException) {
            System.err.println(IOException);
            System.exit(2);
        }

    }               //getTrainInfo


}
