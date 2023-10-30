package ar.edu.itba.pod.tp2.mappers;

import ar.edu.itba.pod.tp2.models.Ride;
import ar.edu.itba.pod.tp2.models.Station;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.IMap;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Query1Mapper implements Mapper<String, Ride, String, Long>, HazelcastInstanceAware {
    private static final Long ONE = 1L;
    private static final Logger logger = LoggerFactory.getLogger(Query1Mapper.class);
    private transient HazelcastInstance hazelcastInstance;
    @Override
    public void map(String key, Ride ride, Context<String, Long> context) {
        IMap<Long, Station> map = hazelcastInstance.getMap("g9-map");
        Station startStation = map.get(ride.getStartPk());
        Station endStation = map.get(ride.getEndPk());
        if (ride.getStartPk() != ride.getEndPk() && startStation != null && endStation != null) {
            String s = startStation.getName() + ";" + endStation.getName();
            context.emit(s,ONE);
        }
    }

    @Override
    public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
        this.hazelcastInstance = hazelcastInstance;
    }
}
