package GetTrainInfo;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.*;

public class GetTrainInfo  implements Serializable{
    public static boolean getTrainInfo(BufferedWriter writer, String url, String phantomjsPath) {

        try {
            Runtime rt = Runtime.getRuntime();
            Process phantomjs = rt.exec(phantomjsPath + "phantomjs.exe " + phantomjsPath + "code.js " + url);

            InputStream inputStream = phantomjs.getInputStream();
            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
            StringBuilder doc = new StringBuilder();
            String tmp;
            while ((tmp = reader.readLine()) != null) {
                doc.append(tmp);
                //System.out.println(tmp);
            }


            Elements tbody = Jsoup.parse(doc.toString(), "UTF-8").getElementsByClass("tbody");
            if(tbody.size()==0){
                System.out.println(url+"\nThe two stations has no direct train!\n");
                return false;

            }

            for (Element i : tbody) {

                String train = i.getElementsByClass("w1").text().split(" ")[0];
                String times[] = i.getElementsByClass("w2").text().split(" ");
                String stations[] = i.getElementsByClass("w3").text().split(" ");

                String startTime = times[0];
                String arriveTime = times[1];
                int day = times.length == 2 ? 0 : Integer.parseInt(times[2]);
                String startStation = stations[1];
                String arriveStation = stations[3];


//                System.out.print("train: " + train + "\t\t"); //车次
//                System.out.print("start time: " + startTime + "\t\t");
//                System.out.print("arrive time: " + arriveTime + "\t\t");
//                System.out.print("+" + day + "\t");
//                System.out.print("start station: " + startStation + "\t\t");
//                System.out.println("arrive station: " + arriveStation);          //


                writer.write(train + "," + startTime + "," + arriveTime + "," + day + "," + startStation + "," + arriveStation + "\n");
                writer.flush();
                reader.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return true;
    }               //getTrainInfo
}
