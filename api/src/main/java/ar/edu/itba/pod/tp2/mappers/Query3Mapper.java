
package ar.edu.itba.pod.tp2.mappers;

        import ar.edu.itba.pod.tp2.models.Query3Value;
        import ar.edu.itba.pod.tp2.models.Ride;
        import ar.edu.itba.pod.tp2.models.Station;
        import com.hazelcast.core.HazelcastInstance;
        import com.hazelcast.core.HazelcastInstanceAware;
        import com.hazelcast.core.IMap;
        import com.hazelcast.mapreduce.Context;
        import com.hazelcast.mapreduce.Mapper;
        import org.slf4j.Logger;
        import org.slf4j.LoggerFactory;
        import java.time.Duration;


public class Query3Mapper implements Mapper<String, Ride, String, Query3Value>, HazelcastInstanceAware {
    private static final Logger logger = LoggerFactory.getLogger(Query3Mapper.class);
    private transient HazelcastInstance hazelcastInstance;

    @Override
    public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
        this.hazelcastInstance = hazelcastInstance;
    }

    @Override
    public void map(String key, Ride ride, Context<String, Query3Value> context) {
        IMap<Long, Station> map = hazelcastInstance.getMap("g9-query3-map");
        Station startStation = map.get(ride.getStartPk());
        Station endStation = map.get(ride.getEndPk());
        if (ride.getStartPk() != ride.getEndPk() && startStation != null && endStation != null) {
            String s = startStation.getName() + ";" + endStation.getName();
            long rideTime = Duration.between(ride.getStartDate(), ride.getEndDate()).toMinutes();
            Query3Value value = new Query3Value(rideTime, ride.getStartDate());
            context.emit(s, value);
        }
    }

}