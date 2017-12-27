package GetTrainInfo;

import java.io.*;

public class MutilThreadingGetTrainInfo {

    public static void main(String[] agrs) {
        Long begin = System.currentTimeMillis();

        final String phantomjsPath = "C:\\Users\\rabbin\\Desktop\\spark\\rabbin\\conf\\";
        final String trainsInfo = "C:\\Users\\rabbin\\Desktop\\spark\\rabbin\\file\\MutiThreading\\";
        final String stationPairs = "C:\\Users\\rabbin\\Desktop\\spark\\rabbin\\file\\stationPairs.txt";

        new MutilThreadingGetTrainInfo(phantomjsPath, trainsInfo, stationPairs).begin();

        Long end = System.currentTimeMillis();
        Long time = (end - begin) / 1000;
        System.out.println("The program has run " + time / 60 + " mins " + (end - begin) % 60 + " seconds");
    }           //main


    private BufferedWriter TrainInfoWriter = null;
    private BufferedReader reader = null;
    private static BufferedWriter StationsExitsWriter = null;

    public static int UrlNum = 0;

    public  String phantomjsPath = null;

    MutilThreadingGetTrainInfo(String phantomjsPath, String trainsInfo, String stationPairs) {

        this.phantomjsPath = phantomjsPath;
        try {
            this.reader = new BufferedReader(new FileReader(new File(stationPairs)));
            int id = new File(trainsInfo+ "traininfo\\").listFiles().length + 1;
            this.StationsExitsWriter= new BufferedWriter(new FileWriter(new File(trainsInfo+"stations\\"+id+".txt")));
            this.TrainInfoWriter = new BufferedWriter(new FileWriter(new File(trainsInfo + "traininfo\\"+id + ".txt")));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void begin() {

        try {
            String line;
            line = reader.readLine();
            while (line != null) {
                if (ParseJs.ThreadCount > ParseJs.MaxThreadCount) {
                    Thread.sleep(5000);
                    continue;
                }

                new ParseJs(TrainInfoWriter,StationsExitsWriter, line,phantomjsPath).start();
                synchronized (ParseJs.ThreadCount) {
                    ParseJs.ThreadCount++;
                }
                line = reader.readLine();
            }               //while

        } catch (Exception e) {
            e.printStackTrace();
        }


        while (ParseJs.ThreadCount!=0){
          try{
              Thread.sleep(1000);
          }catch(Exception e){
              e.printStackTrace();
          }
        }
        try {
            TrainInfoWriter.close();
            StationsExitsWriter.close();

        } catch (IOException e) {
            e.printStackTrace();
        }

    }

}


class ParseJs extends Thread {
    private static final String splitChar = ",";

    public static Integer ThreadCount = 0;
    public final static int MaxThreadCount = 8;

    private String line;
    private BufferedWriter TrainInfoWriter;
    private BufferedWriter StationsExitsWriter;

    private String phantomjsPath;
    public ParseJs(BufferedWriter TrainInfoWriter,BufferedWriter StationsExitsWriter, String line,String phantomjsPath) {
        this.line = line;
        this.TrainInfoWriter = TrainInfoWriter;
        this.StationsExitsWriter=StationsExitsWriter;
        this.phantomjsPath= phantomjsPath;
    }

    public void run() {
        //System.out.println("now in thread " + Thread.currentThread().getName());
        String[] stations = line.split(splitChar);
        String url = "http://trains.ctrip.com/TrainBooking/Search.aspx?from=" + stations[1] + "&to=" + stations[3] + "&day=2";
        GetTrainInfo.getTrainInfo(TrainInfoWriter, url,phantomjsPath);
        try{
            if(GetTrainInfo.getTrainInfo(TrainInfoWriter, url, phantomjsPath)){
                StationsExitsWriter.write(stations[1]+","+stations[3]+"\n");
                StationsExitsWriter.flush();
            }
        }catch (Exception e){
            e.printStackTrace();
        }
        MutilThreadingGetTrainInfo.UrlNum++;
        System.out.println("****" + MutilThreadingGetTrainInfo.UrlNum + "*******");
        synchronized (ThreadCount) {
            ThreadCount--;
        }
    }           //run

}
