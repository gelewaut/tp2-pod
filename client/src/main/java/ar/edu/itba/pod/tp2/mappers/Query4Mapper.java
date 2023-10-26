package ar.edu.itba.pod.tp2.mappers;

import ar.edu.itba.pod.tp2.models.Ride;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Query4Mapper implements Mapper<String, Ride, String, Long> {
    private static final Long ONE = 1L;
    private static final Logger logger = LoggerFactory.getLogger(Query4Mapper.class);
    @Override
    public void map(String key, Ride ride, Context<String, Long> context) {
        if (ride.getStartPk() != ride.getEndPk()) {
            context.emit(String.valueOf(ride.getStartPk()), -1L);
            context.emit(String.valueOf(ride.getEndPk()), 1L);
        } else {
            context.emit(String.valueOf(ride.getStartPk()), 0L);
        }
    }
}