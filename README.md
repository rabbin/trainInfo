this is a java program based on spark 2.20 ,just for learning spark
==
1.the dependencies is illustrated in the file [pom.xml](https://github.com/rabbin/TrainInfo/blob/master/pom.xml)
--
2.src
--
  * GetTrainCode.java:<p>
    >get the code related to a specific station.
    ><br> for example,"shanghai" and "beijing" is the codes of "北京"` and `"上海" int the link : http://trains.ctrip.com/TrainBooking/Search.aspx?from=beijing&to=zhengzhou&day=1
    ><br> all the codes and stations are in the file [station](https://github.com/rabbin/TrainInfo/blob/master/files/station) or on the website http://webresource.c-ctrip.com/ResTrainOnline/R1/TrainBooking/JS/station_gb2312.js
