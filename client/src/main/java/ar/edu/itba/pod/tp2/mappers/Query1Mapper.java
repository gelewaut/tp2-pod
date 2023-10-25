package ar.edu.itba.pod.tp2.mappers;

import ar.edu.itba.pod.tp2.models.Ride;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

public class Query1Mapper implements Mapper<String, Ride, String, Long> {
    private static final Long ONE = 1L;
    @Override
    public void map(String key, Ride ride, Context<String, Long> context) {
        if (ride.startPk() != ride.endPk()) {
            String s = ride.startPk() + ":" + ride.endPk();
            context.emit(s,ONE);
        }
    }
}
