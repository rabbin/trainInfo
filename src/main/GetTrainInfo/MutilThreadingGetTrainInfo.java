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


    private BufferedWriter writer = null;
    private BufferedReader reader = null;

    public static int UrlNum = 0;

    public  String phantomjsPath = null;

    MutilThreadingGetTrainInfo(String phantomjsPath, String trainsInfo, String stationPairs) {

        this.phantomjsPath = phantomjsPath;
        try {
            this.reader = new BufferedReader(new FileReader(new File(stationPairs)));
            int id = new File(trainsInfo).listFiles().length + 1;
            this.writer = new BufferedWriter(new FileWriter(new File(trainsInfo + id + ".txt")));
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

                new ParseJs(writer, line,phantomjsPath).start();
                synchronized (ParseJs.ThreadCount) {
                    ParseJs.ThreadCount++;

                }
                line = reader.readLine();
            }               //while

        } catch (Exception e) {
            e.printStackTrace();
        }


    }

}


class ParseJs extends Thread {
    private static final String splitChar = ",";

    public static Integer ThreadCount = 0;
    public final static int MaxThreadCount = 8;

    private String line;
    private BufferedWriter writer;
    private String phantomjsPath;
    public ParseJs(BufferedWriter writer, String line,String phantomjsPath) {
        this.line = line;
        this.writer = writer;
        this.phantomjsPath= phantomjsPath;
    }

    public void run() {
        System.out.println("now in thread " + Thread.currentThread().getName());
        String[] stations = line.split(splitChar);
        String url = "http://trains.ctrip.com/TrainBooking/Search.aspx?from=" + stations[1] + "&to=" + stations[3] + "&day=2";
        System.out.println(url);
        GetTrainInfo.getTrainInfo(writer, url,phantomjsPath);
        MutilThreadingGetTrainInfo.UrlNum++;
        System.out.println("****" + MutilThreadingGetTrainInfo.UrlNum + "*******");
        synchronized (ThreadCount) {
            ThreadCount--;
        }
    }           //run

}
