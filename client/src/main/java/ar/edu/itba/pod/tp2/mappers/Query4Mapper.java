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

import java.time.LocalDate;

public class Query4Mapper implements Mapper<String, Ride, String, Long>, HazelcastInstanceAware {
    private static final Long ONE = 1L;
    private static final Logger logger = LoggerFactory.getLogger(Query4Mapper.class);
    private transient HazelcastInstance hazelcastInstance;

    @Override
    public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
        this.hazelcastInstance = hazelcastInstance;
    }

    @Override
    public void map(String key, Ride ride, Context<String, Long> context) {
        IMap<Long, Station> map = hazelcastInstance.getMap("g9-map");
        IMap<String, LocalDate> map2 = hazelcastInstance.getMap("g9-query-4-dates");
        Station startStation = map.get(ride.getStartPk());
        Station endStation = map.get(ride.getEndPk());
        LocalDate startDate = map2.get("startDate");
        LocalDate endDate = map2.get("endDate");
        if (startStation != null && endStation != null && !startDate.isAfter(ride.getStartDate().toLocalDate()) && !endDate.isBefore(ride.getEndDate().toLocalDate())) {
            if (ride.getStartPk() != ride.getEndPk()) {
                context.emit(startStation.getName(), -1L);
                context.emit(endStation.getName(), 1L);
            } else {
                context.emit(startStation.getName(), 0L);
            }
        }
    }
}