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
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IList;
import com.hazelcast.core.IMap;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class Client1 {
    static Map<Integer, Station> stations = new HashMap<>();
    static List<Ride> rides = new ArrayList<>();

    private static final Logger logger = LoggerFactory.getLogger(Client1.class);
    public static void main(String[] args) {
        Properties props = System.getProperties();
        String addresses[] = props.getProperty("addresses").split(";");
        String inPath = props.getProperty("inPath");
        String outPath = props.getProperty("outPath");


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
//        final KeyValueSource<String, String> source = KeyValueSource.fromMap(map);
        final IList<Ride> list = hazelcastInstance.getList("g9-list");
        final KeyValueSource<String, Ride> source = KeyValueSource.fromList(list);

        JobTracker jobTracker = hazelcastInstance.getJobTracker("word-count");

        Job<String, Ride> job = jobTracker.newJob( source );
        ICompletableFuture<Map<String, Long>> future = job
                .mapper(new Query1Mapper() )
                .reducer( new Query1ReducerFactory() )
                .submit();

        // Attach a callback listener
//        future.andThen( buildCallback() );
        // Wait and retrieve the result
//        Map<String, Long> result = future.get();

        HazelcastClient.shutdownAll();
    }

    private void query() {

    }
}
