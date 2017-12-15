package Unility;


import java.io.*;
import java.util.LinkedList;

public class GetTrainCode {
    public static final String REGEX = "[|]+";
    public static final String station = "C:\\Users\\rabbin\\Desktop\\spark\\rabbin\\file\\station.txt";
    public static final String stations = "C:\\Users\\rabbin\\Desktop\\spark\\rabbin\\file\\stations.txt";
    public static final String stationPairs = "C:\\Users\\rabbin\\Desktop\\spark\\rabbin\\file\\stationPairs.txt";
//    String url = "http://webresource.c-ctrip.com/ResTrainOnline/R1/TrainBooking/JS/station_gb2312.js";
    public static void main(String[] args) {

//        GetTrainCode.getTrainCode(station,stations);
        GetTrainCode.getTrainPair(stations,stationPairs);
        System.out.println("###");
    }

    public static void getTrainCode(String station,String stations) {
        String content = null;
        try (BufferedReader reader = new BufferedReader(new FileReader(new File(station)))) {
            content = reader.readLine();
        } catch (IOException IOException) {
            System.err.println("can't read the file " + station + IOException);
        }

        String[] sta = content.split(GetTrainCode.REGEX);

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(new File(stations)))) {
            for (int i = 0; i < sta.length; i = i + 5) {
                System.out.println("stastion:" + sta[i] + "\tcode:" + sta[i + 1]);
                writer.write(sta[i] + "," + sta[i + 1] + "\n");
                writer.flush();
            }
        } catch (IOException IOExcepion) {
            System.err.println(IOExcepion);
        }
    }           //getTrainCode

    public static void getTrainPair(String stations,String stationPairs) {

        try (BufferedReader reader = new BufferedReader(new FileReader(new File(stations)));
             BufferedWriter writer = new BufferedWriter(new FileWriter(new File(stationPairs)));) {

            LinkedList<String> stas = new LinkedList<String>();
            String line = null;

            while ((line = reader.readLine()) != null) {
                stas.add(line);
            }

            for (int i = 0; i < stas.size(); i++) {
                for (int j = 0; j < stas.size(); j++) {
                    if (i != j) {
                        //System.out.println(stas.get(i)+","+stas.get(j));
                        System.out.println(i);
                        writer.write(stas.get(i) + "," + stas.get(j) + "\n");
                        writer.flush();
                    }       //if
                }           //j
            }               //i
        } catch (Exception IOException) {
            System.err.println(IOException);
        }
    }           //getTrainPair

}               //GetTrainCode
