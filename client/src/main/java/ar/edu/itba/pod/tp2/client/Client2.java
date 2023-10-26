package ar.edu.itba.pod.tp2.client;

import ar.edu.itba.pod.tp2.mappers.Query2Mapper;
import ar.edu.itba.pod.tp2.models.Ride;
import ar.edu.itba.pod.tp2.models.Station;
import ar.edu.itba.pod.tp2.reducers.Query2ReducerFactory;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.core.*;
import com.hazelcast.mapreduce.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.*;

public class Client2 {
    private static final Logger logger = LoggerFactory.getLogger(Client1.class);
    public static void main(String[] args) {
//        Properties props = System.getProperties();
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

        JobTracker jobTracker = hazelcastInstance.getJobTracker("Query2");

        Job<String, Ride> job = jobTracker.newJob( source );
//        ICompletableFuture<Map<String, Long>> future = job
//                .mapper(new Query2Mapper() )
//                .reducer( new Query2ReducerFactory() )
//                .submit();

        // Wait and retrieve the result
//        try{
//            Map<String, Long> result = future.get();
//            for (Map.Entry<String, Long> entry: result.entrySet()) {
//                logger.info(entry.getKey() + ": " + entry.getValue());
//            }
//            printResult(result, map, outPath);
//        } catch (Exception ex) {
//            logger.error(ex.getMessage());
//        }

        map.clear();
        list.clear();
        HazelcastClient.shutdownAll();
    }

    private static void printResult(Map<String,Long> result, IMap<Integer, Station> map, String outPath) throws IOException {
        FileWriter file = new FileWriter(outPath+"query1.csv");
        PrintWriter filePrinter = new PrintWriter(file);
        filePrinter.println("FALTA ORDENAR E IMPRIMIR BIEN");
        filePrinter.println("station_a;station_b;trips_between_a_b");
        for (Map.Entry<String, Long> entry: result.entrySet()) {
            String[] pks = entry.getKey().split(":");
        }
        filePrinter.close();
    }
}
