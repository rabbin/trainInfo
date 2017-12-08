package Unility;
//        String url = "http://webresource.c-ctrip.com/ResTrainOnline/R1/TrainBooking/JS/station_gb2312.js";

import java.io.*;
import java.util.LinkedList;

public class GetTrainCode {
    public static final String REGEX = "[|]+";
    public static final String station = "C:\\Users\\rabbin\\Desktop\\spark\\rabbin\\file\\station.txt";
    public static final String stations = "C:\\Users\\rabbin\\Desktop\\spark\\rabbin\\file\\stations.txt";
    public static final String stationPairs = "C:\\Users\\rabbin\\Desktop\\spark\\rabbin\\file\\stationPairs.txt";

    public static void main(String[] args) {

        //GetTrainCode.getTrainCode();
        // GetTrainCode.getTrainPair();
        System.out.println("###");
    }

    public static void getTrainCode() {
        String content = null;
        try {
            BufferedReader reader = new BufferedReader(new FileReader(new File(GetTrainCode.station)));
            content = reader.readLine();
            reader.close();

        } catch (FileNotFoundException FileNotFound) {
            System.err.println("File not found!" + FileNotFound);
            System.exit(1);
        } catch (IOException IOException) {
            System.err.println("can't read the file " + GetTrainCode.station + IOException);
        }

        String[] stations = content.split(GetTrainCode.REGEX);

        try {

            BufferedWriter writer = new BufferedWriter(new FileWriter(new File(GetTrainCode.stations)));
            for (int i = 0; i < stations.length; i = i + 5) {
                System.out.println("stastion:" + stations[i] + "\tcode:" + stations[i + 1]);
                writer.write(stations[i] + "," + stations[i + 1] + "\n");
                writer.flush();
            }
            writer.close();
        } catch (IOException IOExcepion) {
            System.err.println(IOExcepion);
        }

    }           //getTrainCode


    public static void getTrainPair() {

        try {
            BufferedReader reader = new BufferedReader(new FileReader(new File(GetTrainCode.stations)));

            LinkedList<String> stations = new LinkedList<String>();

            String line = null;
            while ((line = reader.readLine()) != null) {
                stations.add(line);
            }
            reader.close();

            BufferedWriter writer = new BufferedWriter(new FileWriter(new File(GetTrainCode.stationPairs)));

            for (int i = 0; i < stations.size(); i++) {
                for (int j = 0; j < stations.size(); j++) {
                    if (i != j) {
                        //System.out.println(stations.get(i)+","+stations.get(j));
                        System.out.println(i);
                        writer.write(stations.get(i) + "," + stations.get(j) + "\n");
                        writer.flush();
                    }

                }           //j
            }               //i
            writer.close();

        } catch (Exception IOException) {
            System.err.println(IOException);
        }


    }           //getTrainPair


}
