package ar.edu.itba.pod.tp2.client;

import ar.edu.itba.pod.tp2.mappers.Query1Mapper;
import ar.edu.itba.pod.tp2.models.Ride;
import ar.edu.itba.pod.tp2.models.Station;
import ar.edu.itba.pod.tp2.reducers.Query1ReducerFactory;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.core.*;
import com.hazelcast.mapreduce.*;
import com.hazelcast.ringbuffer.ReadResultSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.*;

public class Client1 {
    private static final Logger logger = LoggerFactory.getLogger(Client1.class);
    public static void main(String[] args) {
        Properties props = System.getProperties();
//        String addresses[] = props.getProperty("addresses").split(";");
//        String inPath = props.getProperty("inPath");
//        String outPath = props.getProperty("outPath");
        String[] addresses = {"127.0.0.1:5701"};
        String inPath = "C:\\Users\\gonel\\Documents\\POD\\tpe2-g9\\client\\src\\main\\resources\\";
        String outPath = "C:\\Users\\gonel\\Documents\\POD\\tpe2-g9\\client\\src\\main\\resources\\";


        // Client Config
        ClientConfig clientConfig = new ClientConfig();
        // Group Config
        GroupConfig groupConfig = new GroupConfig()
                .setName("G9")
                .setPassword("G09-pass");

        clientConfig.setGroupConfig(groupConfig);
        ClientNetworkConfig clientNetworkConfig = new ClientNetworkConfig();
        clientNetworkConfig.addAddress(addresses);
        clientConfig.setNetworkConfig(clientNetworkConfig);
        HazelcastInstance hazelcastInstance = HazelcastClient.newHazelcastClient(clientConfig);

        new DataParser().readFile(hazelcastInstance, inPath);
        IMap<Integer, Station> map = hazelcastInstance.getMap("g9-map");
        IList<Ride> list = hazelcastInstance.getList("g9-list");

//        final KeyValueSource<Integer, Station> source = KeyValueSource.fromMap(map);
        final KeyValueSource<String, Ride> source = KeyValueSource.fromList(list);

        JobTracker jobTracker = hazelcastInstance.getJobTracker("Query1");

        Job<String, Ride> job = jobTracker.newJob( source );
        ICompletableFuture<Map<String, Long>> future = job
                .mapper(new Query1Mapper() )
                .reducer( new Query1ReducerFactory() )
                .submit();

        // Wait and retrieve the result
        try{
            Map<String, Long> result = future.get();
            for (Map.Entry<String, Long> entry: result.entrySet()) {
                logger.info(entry.getKey() + ";" + entry.getValue());
            }
            printResult(result, outPath);
        } catch (Exception ex) {
            logger.error(ex.getMessage());
        }

        map.clear();
        list.clear();
        HazelcastClient.shutdownAll();
    }

    private static void printResult(Map<String,Long> result, String outPath) throws IOException {
        FileWriter file = new FileWriter(outPath+"query1.csv");
        PrintWriter filePrinter = new PrintWriter(file);
        filePrinter.println("station_a;station_b;trips_between_a_b");
        List<Map.Entry<String, Long>> sortedEntries = result.entrySet().stream().sorted((entry1, entry2) -> {
            int longComparison = Long.compare(entry2.getValue(), entry1.getValue());
            if (longComparison != 0) {
                return longComparison;
            } else {
                return entry1.getKey().compareTo(entry2.getKey());
            }
        }).toList();
        for (Map.Entry<String, Long> entry: sortedEntries) {
            filePrinter.println(entry.getKey()+";"+entry.getValue());
        }
        filePrinter.close();
    }
}
