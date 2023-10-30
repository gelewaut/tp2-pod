package ar.edu.itba.pod.tp2.client;

import ar.edu.itba.pod.tp2.mappers.Query1Mapper;
import ar.edu.itba.pod.tp2.mappers.Query3Mapper;
import ar.edu.itba.pod.tp2.models.Query3Value;
import ar.edu.itba.pod.tp2.models.Ride;
import ar.edu.itba.pod.tp2.models.Station;
import ar.edu.itba.pod.tp2.reducers.Query1ReducerFactory;
import ar.edu.itba.pod.tp2.reducers.Query3ReducerFactory;
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

public class Client3 {
    private static final Logger logger = LoggerFactory.getLogger(Client1.class);
    public static void main(String[] args) {
        Properties props = System.getProperties();
//        String addresses[] = props.getProperty("addresses").split(";");
//        String inPath = props.getProperty("inPath");
//        String outPath = props.getProperty("outPath");
        String[] addresses = {"127.0.0.1:5701"};
        String inPath = "/Users/juaarias/Documents/PAW/tp2-pod/client/src/main/resources/";
        String outPath = "/Users/juaarias/Documents/PAW/tp2-pod/client/src/main/resources/";


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
        IMap<Long, Station> map = hazelcastInstance.getMap("g9-map");
        IList<Ride> list = hazelcastInstance.getList("g9-list");

//        final KeyValueSource<Integer, Station> source = KeyValueSource.fromMap(map);
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

        map.clear();
        list.clear();
        HazelcastClient.shutdownAll();
    }

    private static void printResult(Map<String,Query3Value> result, String outPath) throws IOException {
        FileWriter file = new FileWriter(outPath+"query3.csv");
        PrintWriter filePrinter = new PrintWriter(file);
        filePrinter.println("start_station;end_station;start_date;minutes");
        result.entrySet().stream().sorted((entry1, entry2) -> {
            int aux = entry2.getValue().getMinutes().compareTo(entry1.getValue().getMinutes());
            if (aux == 0) {
                return entry1.getKey().compareTo(entry2.getKey());
            }
            return aux;
        });
        Set<String> keys = result.keySet();
        for (String key : keys) {
            filePrinter.println(key + ";" + result.get(key).getStartDate() + ";" + result.get(key).getMinutes());
        }
        filePrinter.close();
    }
}
