package ar.edu.itba.pod.tp2.client;

import ar.edu.itba.pod.tp2.collators.Query2Collator;
import ar.edu.itba.pod.tp2.combiners.Query2CombinerFactory;
import ar.edu.itba.pod.tp2.mappers.Query2Mapper;
import ar.edu.itba.pod.tp2.models.Ride;
import ar.edu.itba.pod.tp2.models.Station;
import ar.edu.itba.pod.tp2.reducers.Query2ReducerCombinerFactory;
import ar.edu.itba.pod.tp2.reducers.Query2ReducerFactory;
import ar.edu.itba.pod.tp2.reducers.Query4ReducerCombinerFactory;
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
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class Client2 {
    private static final Logger logger = LoggerFactory.getLogger(Client2.class);
    public static void main(String[] args) {
        Properties props = System.getProperties();
        String inPath = props.getProperty("inPath");
        String outPath = props.getProperty("outPath");
        String address = props.getProperty("addresses");

        if (inPath == null || outPath == null || address == null || props.getProperty("n") == null) {
            logger.error("Missing Arguments");
            return;
        }
        int n = Integer.parseInt(props.getProperty("n"));
        String[] addresses = address.split(";");

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

        try {
            FileWriter file = new FileWriter(outPath+"time2.txt");
            PrintWriter filePrinter = new PrintWriter(file);

            filePrinter.println(LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss:SSSS")) + " - Inicio de la lectura del archivo");
            logger.info("Inicio de la lectura del archivo");
            new DataParser().readFile(hazelcastInstance, inPath, "g9-query2-map", "g9-query2-list");
            filePrinter.println(LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss:SSSS")) + " - Fin de lectura del archivo");
            logger.info("Fin de lectura del archivo");

            IMap<Integer, Station> map = hazelcastInstance.getMap("g9-query2-map");
            IList<Ride> list = hazelcastInstance.getList("g9-query2-list");

            filePrinter.println(LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss:SSSS")) + " - Inicio del trabajo map/reduce");
            logger.info("Inicio del trabajo map/reduce");

            final KeyValueSource<String, Ride> source = KeyValueSource.fromList(list);

            JobTracker jobTracker = hazelcastInstance.getJobTracker("Query2");

            Job<String, Ride> job = jobTracker.newJob( source );
            ICompletableFuture<List<Map.Entry<String,Double>>> future;

            if (props.getProperty("c") == null) {
                future = job
                        .mapper(new Query2Mapper() )
                        .reducer(new Query2ReducerFactory())
                        .submit(new Query2Collator());
            } else {
                future = job
                        .mapper(new Query2Mapper())
                        .combiner(new Query2CombinerFactory())
                        .reducer(new Query2ReducerCombinerFactory())
                        .submit(new Query2Collator());
            }

            // Wait and retrieve the result
            try{
                List<Map.Entry<String,Double>> result = future.get();
                printResult(result, outPath, n);
            } catch (Exception ex) {
                logger.error(ex.getMessage());
            }

            filePrinter.println(LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss:SSSS")) + " - Fin del trabajo map/reduce");
            logger.info("Fin del trabajo map/reduce");
            filePrinter.close();

            map.clear();
            list.clear();
        } catch (Exception ex) {
            logger.error(ex.getMessage());
        }

        HazelcastClient.shutdownAll();
    }


    private static void printResult(List<Map.Entry<String,Double>> result, String outPath, int n) throws IOException {
        FileWriter file = new FileWriter(outPath+"query2.csv");
        PrintWriter filePrinter = new PrintWriter(file);
        filePrinter.println("station_a;avg_distance");
        int i = 0;
        for (Map.Entry<String, Double> entry: result) {
            if (i < n ) {
                filePrinter.println(entry.getKey() + ";" + String.format("%.2f", entry.getValue()));
            }else {
                break;
            }
            i++;
        }
        filePrinter.close();
    }
}
