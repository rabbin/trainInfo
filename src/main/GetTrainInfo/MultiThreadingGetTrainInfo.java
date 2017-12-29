package GetTrainInfo;

import java.io.*;

public class MultiThreadingGetTrainInfo {

    private static final String splitChar = ",";
    private String phantomjsPath;
    private static int UrlNum = 0;

    private static Integer ThreadCount = 0;
    private final static int MaxThreadCount = 8;

    private BufferedWriter TrainInfoWriter;
    private BufferedWriter StationsExitsWriter;
    private BufferedReader reader = null;

    /**
     * 爬虫线程
     */
    private class ParseJs extends Thread {
        private String line;

        ParseJs(String line){
            this.line=line;
        }

        public void run() {
            //System.out.println("now in thread " + Thread.currentThread().getName());
            String[] stations = line.split(splitChar);
            String url = "http://trains.ctrip.com/TrainBooking/Search.aspx?from=" + stations[1] + "&to=" + stations[3] + "&day=2";
            try{
                if(GetTrainInfo.getTrainInfo(TrainInfoWriter, url, phantomjsPath)){
                    StationsExitsWriter.write(stations[1]+","+stations[3]+"\n");
                    StationsExitsWriter.flush();
                }
            }catch (Exception e){
                e.printStackTrace();
            }
            MultiThreadingGetTrainInfo.UrlNum++;
            System.out.println("****" + MultiThreadingGetTrainInfo.UrlNum + "*******");
            synchronized (ThreadCount) {
                ThreadCount--;
            }
        }           //run

    }                   //ParseJS


    MultiThreadingGetTrainInfo(String phantomjsPath, String trainsInfo, String stationPairs) {

        this.phantomjsPath = phantomjsPath;
        try {
            this.reader = new BufferedReader(new FileReader(new File(stationPairs)));
            int id = new File(trainsInfo+ "traininfo\\").listFiles().length + 1;
            this.StationsExitsWriter= new BufferedWriter(new FileWriter(new File(trainsInfo+"stations\\"+id+".txt")));
            this.TrainInfoWriter = new BufferedWriter(new FileWriter(new File(trainsInfo + "traininfo\\"+id + ".txt")));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }                           //MultiThreadingGetTrainInfo

    public void begin() {

        try {
            String line;
            line = reader.readLine();
            while (line != null) {
                if (ThreadCount > MaxThreadCount) {
                    Thread.sleep(5000);
                    continue;
                }               //if

                new ParseJs(line).start();
                synchronized (ThreadCount) {
                    ThreadCount++;
                }
                line = reader.readLine();
            }               //while

        } catch (Exception e) {
            e.printStackTrace();
        }


        while (ThreadCount!=0){
            try{
                Thread.sleep(1000);
            }catch(Exception e){
                e.printStackTrace();
            }
        }                   //while
        try {
            TrainInfoWriter.close();
            StationsExitsWriter.close();

        } catch (IOException e) {
            e.printStackTrace();
        }

    }               //begin



    public static void main(String[] agrs) {
        Long begin = System.currentTimeMillis();

        final String phantomjsPath = "C:\\Users\\rabbin\\Desktop\\spark\\rabbin\\conf\\";
        final String trainsInfo = "C:\\Users\\rabbin\\Desktop\\spark\\rabbin\\file\\MultiThreading\\";
        final String stationPairs = "C:\\Users\\rabbin\\Desktop\\spark\\rabbin\\file\\stationPairs.txt";

        new MultiThreadingGetTrainInfo(phantomjsPath, trainsInfo, stationPairs).begin();

        Long end = System.currentTimeMillis();
        Long time = (end - begin) / 1000;
        System.out.println("The program has run " + time / 60 + " mins " + (end - begin) % 60 + " seconds");
    }           //main


}
