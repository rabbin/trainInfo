package GetTrainInfo;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

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

    public static String phantomjsPath = null;

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

                new ParseJs(writer, line).start();
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

    public ParseJs(BufferedWriter writer, String line) {
        this.line = line;
        this.writer = writer;
    }

    public void run() {
        System.out.println("now in thread " + Thread.currentThread().getName());
        String[] stations = line.split(splitChar);
        String url = "http://trains.ctrip.com/TrainBooking/Search.aspx?from=" + stations[1] + "&to=" + stations[3] + "&day=2";
        System.out.println(url);
        getTrainInfo(writer, url);
        MutilThreadingGetTrainInfo.UrlNum++;
        System.out.println("****" + MutilThreadingGetTrainInfo.UrlNum + "*******");
        synchronized (ThreadCount) {
            ThreadCount--;
        }
    }           //run


    private void getTrainInfo(BufferedWriter writer, String url) {

        try {
            Runtime rt = Runtime.getRuntime();
            Process phantomjs = rt.exec(MutilThreadingGetTrainInfo.phantomjsPath + "phantomjs.exe " + MutilThreadingGetTrainInfo.phantomjsPath + "code.js " + url);

            InputStream inputStream = phantomjs.getInputStream();
            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
            StringBuilder doc = new StringBuilder();
            String tmp;
            while ((tmp = reader.readLine()) != null) {
                doc.append(tmp);
                //System.out.println(tmp);
            }


            Elements tbody = Jsoup.parse(doc.toString(), "UTF-8").getElementsByClass("tbody");

            for (Element i : tbody) {

                String train = i.getElementsByClass("w1").text().split(" ")[0];
                String times[] = i.getElementsByClass("w2").text().split(" ");
                String stations[] = i.getElementsByClass("w3").text().split(" ");

                String startTime = times[0];
                String arriveTime = times[1];
                int day = times.length == 2 ? 0 : Integer.parseInt(times[2]);
                String startStation = stations[1];
                String arriveStation = stations[3];


                System.out.print("train: " + train + "\t\t"); //车次
                System.out.print("start time: " + startTime + "\t\t");
                System.out.print("arrive time: " + arriveTime + "\t\t");
                System.out.print("+" + day + "\t");
                System.out.print("start station: " + startStation + "\t\t");
                System.out.println("arrive station: " + arriveStation);          //


                writer.write(train + "," + startTime + "," + arriveTime + "," + day + "," + startStation + "," + arriveStation + "\n");
                writer.flush();
                reader.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }               //getTrainInfo
}
