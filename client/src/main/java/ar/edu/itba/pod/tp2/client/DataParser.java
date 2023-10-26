package ar.edu.itba.pod.tp2.client;

import ar.edu.itba.pod.tp2.models.Ride;
import ar.edu.itba.pod.tp2.models.Station;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IList;
import com.hazelcast.core.IMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.Reader;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;

public class DataParser {
    private static final Logger logger = LoggerFactory.getLogger(DataParser.class);
    public void readFile (HazelcastInstance instance, String inPath) {

        logger.info("Start Upload Csv " + LocalTime.now());

        File bikesCsv = new File(inPath+"bikes.csv");
        File stationsCsv = new File(inPath+"stations.csv");
        IMap<Integer, Station> stations = instance.getMap("g9-map");
        IList<Ride> rides = instance.getList("g9-list");

        try{
            Reader fileReaderBikes = new FileReader(bikesCsv);
            Reader fileReaderStations = new FileReader(stationsCsv);

            BufferedReader brBikes = new BufferedReader(fileReaderBikes);
            BufferedReader brStations = new BufferedReader(fileReaderStations);

            String lineBikes = brBikes.readLine(); // skip first line
            String lineStations = brStations.readLine(); // skip first line


            while ((lineBikes = brBikes.readLine()) != null) {
                String[] values = lineBikes.split(";");
                Integer isMember = Integer.parseInt(values[4]);

                Ride r = new Ride(
                        LocalDateTime.parse(values[0], DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")),
                        Integer.parseInt(values[1]),
                        LocalDateTime.parse(values[2], DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")),
                        Integer.parseInt(values[3]),
                        isMember==1 ? Boolean.TRUE : Boolean.FALSE
                );
                rides.add(r);
            }

            while ((lineStations = brStations.readLine()) != null) {
                String[] values = lineStations.split(";");
                int pk = Integer.parseInt(values[0]);
                Station s = new Station(pk,
                        values[1],
                        Double.parseDouble(values[2]),
                        Double.parseDouble(values[3]));
                stations.put(pk,s);
            }

            logger.info("Finished loading csv " + LocalTime.now());
        } catch (Exception ex) {
            logger.error(ex.getMessage());
        }
    }
}
