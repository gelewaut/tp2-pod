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

import java.util.Map;
import java.util.Properties;

public class Client4 {
    private static final Logger logger = LoggerFactory.getLogger(Client4.class);
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
        } catch (Exception ex) {
            logger.error(ex.getMessage());
        }

        map.clear();
        list.clear();
        HazelcastClient.shutdownAll();
    }
}
