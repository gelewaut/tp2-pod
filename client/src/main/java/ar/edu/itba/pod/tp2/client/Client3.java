package ar.edu.itba.pod.tp2.client;

import ar.edu.itba.pod.tp2.mappers.Query3Mapper;
import ar.edu.itba.pod.tp2.models.Query3Value;
import ar.edu.itba.pod.tp2.models.Ride;
import ar.edu.itba.pod.tp2.models.Station;
import ar.edu.itba.pod.tp2.reducers.Query3ReducerFactory;
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

public class Client3 {
    private static final Logger logger = LoggerFactory.getLogger(Client1.class);
    public static void main(String[] args) {
        Properties props = System.getProperties();
        String inPath = props.getProperty("inPath");
        String outPath = props.getProperty("outPath");
        String address = props.getProperty("addresses");

        if (inPath == null || outPath == null || address == null) {
            logger.error("Missing Arguments");
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
            FileWriter file = new FileWriter(outPath+"time3.txt");
            PrintWriter filePrinter = new PrintWriter(file);
            filePrinter.println(LocalDateTime.now() + " - Inicio de la lectura del archivo");
            logger.info("Inicio de la lectura del archivo");
            new DataParser().readFile(hazelcastInstance, inPath, "g9-query3-map", "g9-query3-list");
            filePrinter.println(LocalDateTime.now() + " - Fin de lectura del archivo");
            logger.info("Fin de lectura del archivo");

            IMap<Long, Station> map = hazelcastInstance.getMap("g9-query3-map");
            IList<Ride> list = hazelcastInstance.getList("g9-query3-list");

            filePrinter.println(LocalDateTime.now() + " - Inicio del trabajo map/reduce");
            logger.info("Inicio del trabajo map/reduce");

            final KeyValueSource<String, Ride> source = KeyValueSource.fromList(list);
            JobTracker jobTracker = hazelcastInstance.getJobTracker("Query3");

            Job<String, Ride> job = jobTracker.newJob( source );
            ICompletableFuture<Map<String, Query3Value>> future = job
                    .mapper(new Query3Mapper() )
                    .reducer( new Query3ReducerFactory() )
                    .submit();

            // Wait and retrieve the result
            try{
                Map<String, Query3Value> result = future.get();
                for (Map.Entry<String, Query3Value> entry: result.entrySet()) {
                    logger.info(entry.getKey() + ";" + entry.getValue().getMinutes());
                }
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

    private static void printResult(Map<String,Query3Value> result, String outPath) throws IOException {
        FileWriter file = new FileWriter(outPath+"query3.csv");
        PrintWriter filePrinter = new PrintWriter(file);
        filePrinter.println("start_station;end_station;start_date;minutes");
        List<Map.Entry<String, Query3Value>> entries = result.entrySet().stream().sorted((entry1, entry2) -> {
            int aux = entry2.getValue().getMinutes().compareTo(entry1.getValue().getMinutes());
            if (aux == 0) {
                return entry1.getKey().compareTo(entry2.getKey());
            }
            return aux;
        }).toList();
        for (Map.Entry<String, Query3Value> entry : entries) {
            filePrinter.println(entry.getKey() + ";" + entry.getValue().getStartDate() + ";" + entry.getValue().getMinutes());
        }
        filePrinter.close();
    }
}
