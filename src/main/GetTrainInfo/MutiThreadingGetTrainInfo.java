package GetTrainInfo;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.*;

public class MutiThreadingGetTrainInfo extends Thread {


    public static void main(String[] agrs) {
        Long begin = System.currentTimeMillis();

        final String stationPairs = "C:\\Users\\rabbin\\Desktop\\spark\\rabbin\\file\\stationPairs.txt";
        final String trainsInfo = "C:\\Users\\rabbin\\Desktop\\spark\\rabbin\\file\\MutiThreading\\";

        try {
            BufferedReader reader = new BufferedReader(new FileReader(new File(stationPairs)));

            int id = new File(trainsInfo).listFiles().length + 1;
            MutiThreadingGetTrainInfo.writer =
                    new BufferedWriter(new FileWriter(new File(trainsInfo + id + ".txt")));

            String line;
            line = reader.readLine();
            while (line != null) {
                if (MutiThreadingGetTrainInfo.ThreadCount > MutiThreadingGetTrainInfo.MaxThreadCount) {
                    Thread.sleep(5000);
                    continue;
                }

                new MutiThreadingGetTrainInfo(line, MutiThreadingGetTrainInfo.writer).start();
                synchronized (MutiThreadingGetTrainInfo.ThreadCountLock) {
                    MutiThreadingGetTrainInfo.ThreadCount++;

                }
                line = reader.readLine();
            }               //while

        } catch (Exception e) {
            e.printStackTrace();
        }
        Long end = System.currentTimeMillis();
        Long time = (end - begin) / 1000;
        System.out.println("The program has run " + time / 60 + " mins " + (end - begin) % 60 + " seconds");
    }           //main



    public static BufferedWriter writer = null;
    public static int UrlNum = 0;
    private static final String phantomjsPath = "C:\\Users\\rabbin\\Desktop\\spark\\rabbin\\conf\\";
    private static final String splitChar = ",";

    public static int ThreadCount = 0;
    public final static int MaxThreadCount = 8;
    public final static Object ThreadCountLock = new Object();

    private String line;

    MutiThreadingGetTrainInfo(String line, BufferedWriter writer) {
        this.line = line;
    }

    @Override
    public void run() {
        System.out.println("now in thread " + Thread.currentThread().getName());
        String[] stations = line.split(splitChar);
        String url = "http://trains.ctrip.com/TrainBooking/Search.aspx?from=" + stations[1] + "&to=" + stations[3] + "&day=2";
        System.out.println(url);
        GetTrainInfo.getTrainInfo(url,writer);
        //getTrainInfo(url, writer);
        MutiThreadingGetTrainInfo.UrlNum++;
        System.out.println("****" + MutiThreadingGetTrainInfo.UrlNum + "*******");
        synchronized (ThreadCountLock) {
            ThreadCount--;
        }
    }           //run


    private void getTrainInfo(String url, BufferedWriter writer) {

        try {
            Runtime rt = Runtime.getRuntime();
            Process phantomjs = rt.exec(phantomjsPath + "phantomjs.exe " + phantomjsPath + "code.js " + url);

            InputStream inputStream = phantomjs.getInputStream();
            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
            StringBuilder doc = new StringBuilder();
            String tmp;
            while ((tmp = reader.readLine()) != null) {
                doc.append(tmp);
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

