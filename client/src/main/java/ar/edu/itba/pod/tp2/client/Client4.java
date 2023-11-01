package ar.edu.itba.pod.tp2.client;

import ar.edu.itba.pod.tp2.mappers.Query4Mapper;
import ar.edu.itba.pod.tp2.models.FluxValue;
import ar.edu.itba.pod.tp2.models.Ride;
import ar.edu.itba.pod.tp2.models.Station;
import ar.edu.itba.pod.tp2.reducers.Query4ReducerFactory;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IList;
import com.hazelcast.core.IMap;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class Client4 {
    private static final Logger logger = LoggerFactory.getLogger(Client4.class);
    public static void main(String[] args) {
        Properties props = System.getProperties();
        String inPath = props.getProperty("inPath");
        String outPath = props.getProperty("outPath");
        String address = props.getProperty("addresses");

        if (inPath == null || outPath == null || address == null || props.getProperty("startDate") == null || props.getProperty("startDate") == null) {
            logger.error("Missing Arguments");
            return;
        }
        LocalDate startDate = LocalDate.parse(props.getProperty("startDate"), DateTimeFormatter.ofPattern("dd/MM/yyyy"));
        LocalDate endDate = LocalDate.parse(props.getProperty("endDate"), DateTimeFormatter.ofPattern("dd/MM/yyyy"));
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
            FileWriter file = new FileWriter(outPath+"time4.txt");
            PrintWriter filePrinter = new PrintWriter(file);
            filePrinter.println(LocalDateTime.now() + " - Inicio de la lectura del archivo");
            new DataParser().readFile(hazelcastInstance, inPath, "g9-query4-map", "g9-query4-list");
            filePrinter.println(LocalDateTime.now() + " - Fin de lectura del archivo");

            IMap<Integer, Station> map = hazelcastInstance.getMap("g9-query4-map");
            IMap<String, LocalDate> map2 = hazelcastInstance.getMap("g9-query-4-dates");
            map2.clear();
            map2.put("startDate", startDate);
            map2.put("endDate", endDate);
            IList<Ride> list = hazelcastInstance.getList("g9-query4-list");

            filePrinter.println(LocalDateTime.now() + " - Inicio del trabajo map/reduce");

            final KeyValueSource<String, Ride> source = KeyValueSource.fromList(list);

            JobTracker jobTracker = hazelcastInstance.getJobTracker("Query4");

            Job<String, Ride> job = jobTracker.newJob( source );
            ICompletableFuture<Map<String, FluxValue>> future = job
                    .mapper(new Query4Mapper() )
                    .reducer( new Query4ReducerFactory() )
                    .submit();

            // Wait and retrieve the result
            try{
                Map<String, FluxValue> result = future.get();
                for (Map.Entry<String, FluxValue> entry: result.entrySet()) {
                    logger.info(entry.getKey() + ": " + entry.getValue());
                }
                printResult(result, outPath);
            } catch (Exception ex) {
                logger.error(ex.getMessage());
            }

            filePrinter.println(LocalDateTime.now() + " - Fin del trabajo map/reduce");

            map.clear();
            map2.clear();
            list.clear();
        } catch (Exception ex) {
            logger.error(ex.getMessage());
        }

        HazelcastClient.shutdownAll();
    }

    private static void printResult(Map<String, FluxValue> result, String outPath) throws IOException {
        FileWriter file = new FileWriter(outPath+"query4.csv");
        PrintWriter filePrinter = new PrintWriter(file);
        filePrinter.println("station;pos_afflux;neutral_afflux;negative_afflux");
        List<Map.Entry<String, FluxValue>> entries = result.entrySet().stream().sorted((entry1, entry2) -> {
            int aux = Long.compare(entry2.getValue().getPositive(), entry1.getValue().getPositive());
            if (aux == 0) {
                return entry1.getKey().compareTo(entry2.getKey());
            }
            return aux;
        }).toList();
        for (Map.Entry<String, FluxValue> entry : entries) {
            filePrinter.println(entry.getKey() + ";" + entry.getValue());
        }
        filePrinter.close();
    }
}
