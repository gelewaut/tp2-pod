package ar.edu.itba.pod.tp2.client;

import ar.edu.itba.pod.tp2.collators.Query1Collator;
import ar.edu.itba.pod.tp2.combiners.Query1CombinerFactory;
import ar.edu.itba.pod.tp2.mappers.Query1Mapper;
import ar.edu.itba.pod.tp2.models.Ride;
import ar.edu.itba.pod.tp2.models.Station;
import ar.edu.itba.pod.tp2.reducers.Query1ReducerFactory;
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
import java.util.*;

public class Client1 {
    private static final Logger logger = LoggerFactory.getLogger(Client1.class);
    public static void main(String[] args) {
        Properties props = System.getProperties();
        String inPath = props.getProperty("inPath");
        String outPath = props.getProperty("outPath");
        String address = props.getProperty("addresses");

        if (address == null) {
            logger.error("Invalid Address");
            return;
        }

        if (inPath == null) {
            logger.error("Missing inPath");
            return;
        }
        if (outPath == null) {
            logger.error("Missing outPath");
            return;
        }
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
            FileWriter file = new FileWriter(outPath+"time1.txt");
            PrintWriter filePrinter = new PrintWriter(file);
            filePrinter.println(LocalDateTime.now() + " - Inicio de la lectura del archivo");
            logger.info("Inicio de la lectura del archivo");
            new DataParser().readFile(hazelcastInstance, inPath, "g9-query1-map", "g9-query1-list");
            filePrinter.println(LocalDateTime.now() + " - Fin de lectura del archivo");
            logger.info("Fin de lectura del archivo");

            IMap<Integer, Station> map = hazelcastInstance.getMap("g9-query1-map");
            IList<Ride> list = hazelcastInstance.getList("g9-query1-list");

            filePrinter.println(LocalDateTime.now() + " - Inicio del trabajo map/reduce");
            logger.info("Inicio del trabajo map/reduce");

            final KeyValueSource<String, Ride> source = KeyValueSource.fromList(list);
            JobTracker jobTracker = hazelcastInstance.getJobTracker("Query1");

            Job<String, Ride> job = jobTracker.newJob( source );
            ICompletableFuture<Map<String, Long>> future = job
                    .mapper(new Query1Mapper() )
                    .combiner(new Query1CombinerFactory())
                    .reducer(new Query1ReducerFactory() )
                    .submit();

            // Wait and retrieve the result
            try{
                Map<String, Long> result = future.get();
                printResult(result, outPath);
            } catch (Exception ex) {
                logger.error(ex.getMessage());
            }

            filePrinter.println(LocalDateTime.now() + " - Fin del trabajo map/reduce");
            logger.info("Fin del trabajo map/reduce");
            filePrinter.close();

            map.clear();
            list.clear();
        } catch (Exception ex) {
            logger.error(ex.getMessage());
        }
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
