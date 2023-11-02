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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DataParser {
    private static final Logger logger = LoggerFactory.getLogger(DataParser.class);
    public void readFile (HazelcastInstance instance, String inPath, String map, String list) {

        logger.info("Start Upload Csv " + LocalTime.now());

        File bikesCsv = new File(inPath+"bikes.csv");
        File stationsCsv = new File(inPath+"stations.csv");
        IMap<Long, Station> stations = instance.getMap(map);
        IList<Ride> rides = instance.getList(list);
        stations.clear();
        rides.clear();

        try{
            Reader fileReaderBikes = new FileReader(bikesCsv);
            Reader fileReaderStations = new FileReader(stationsCsv);

            BufferedReader brBikes = new BufferedReader(fileReaderBikes);
            BufferedReader brStations = new BufferedReader(fileReaderStations);

            String lineBikes = brBikes.readLine(); // skip first line
            String lineStations = brStations.readLine(); // skip first line

            Map<Long, Station> auxMap = new HashMap<>();
            List<Ride> auxList = new ArrayList<>();

            logger.info("Loading stations");
            int i = 0;
            while ((lineStations = brStations.readLine()) != null) {
                String[] values = lineStations.split(";");
                long pk = Long.parseLong(values[0]);
                Station s = new Station(pk,
                        values[1],
                        Double.parseDouble(values[2]),
                        Double.parseDouble(values[3]));
                auxMap.put(pk,s);
                if (i > 1000) {
                    stations.putAll(auxMap);
                    auxMap = new HashMap<>();
                    i = 0;
                }
                i++;
            }
            stations.putAll(auxMap);
            i = 0;

            logger.info("loading rides");
            while ((lineBikes = brBikes.readLine()) != null) {
                String[] values = lineBikes.split(";");
                Integer isMember = Integer.parseInt(values[4]);

                Ride r = new Ride(
                        LocalDateTime.parse(values[0], DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")),
                        Long.parseLong(values[1]),
                        LocalDateTime.parse(values[2], DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")),
                        Long.parseLong(values[3]),
                        isMember==1 ? Boolean.TRUE : Boolean.FALSE
                );
                auxList.add(r);
                if (i > 1000) {
                    rides.addAll(auxList);
                    auxList = new ArrayList<>();
                    i = 0;
                }
                i++;
            }
            rides.addAll(auxList);

            logger.info("Finished loading csv " + LocalTime.now());
        } catch (Exception ex) {
            logger.error(ex.getMessage());
        }
    }
}
