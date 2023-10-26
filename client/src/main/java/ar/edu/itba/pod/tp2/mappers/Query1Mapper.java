package ar.edu.itba.pod.tp2.mappers;

import ar.edu.itba.pod.tp2.models.Ride;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Query1Mapper implements Mapper<String, Ride, String, Long> {
    private static final Long ONE = 1L;
    private static final Logger logger = LoggerFactory.getLogger(Query1Mapper.class);
    @Override
    public void map(String key, Ride ride, Context<String, Long> context) {
        if (ride.getStartPk() != ride.getEndPk()) {
            String s = ride.getStartPk() + ":" + ride.getEndPk();
            context.emit(s,ONE);
        }
    }
}
