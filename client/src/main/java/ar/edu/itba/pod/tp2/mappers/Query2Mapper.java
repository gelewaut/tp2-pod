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

public class Query2Mapper implements Mapper<String, Ride, String, Double>, HazelcastInstanceAware {
    private static final Logger logger = LoggerFactory.getLogger(Query2Mapper.class);
    private transient HazelcastInstance hazelcastInstance;
    @Override
    public void map(String key, Ride ride, Context<String, Double> context) {
        IMap<Long, Station> map = hazelcastInstance.getMap("g9-map");
        Station startStation = map.get(ride.getStartPk());
        Station endstation = map.get(ride.getEndPk());
        if (startStation != null && endstation != null){
            String name = startStation.getName();
            Double distance = haversine(startStation.getLatitude(),
                    startStation.getLongitude(),
                    endstation.getLatitude(),
                    endstation.getLongitude());
            context.emit( name, distance);
        }

    }

    @Override
    public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
        this.hazelcastInstance = hazelcastInstance;
    }

    private double haversine(double lat1, double lon1,
                             double lat2, double lon2)
    {
        // distance between latitudes and longitudes
        double dLat = Math.toRadians(lat2 - lat1);
        double dLon = Math.toRadians(lon2 - lon1);

        // convert to radians
        lat1 = Math.toRadians(lat1);
        lat2 = Math.toRadians(lat2);

        // apply formulae
        double a = Math.pow(Math.sin(dLat / 2), 2) +
                Math.pow(Math.sin(dLon / 2), 2) *
                        Math.cos(lat1) *
                        Math.cos(lat2);
        double rad = 6371;
        double c = 2 * Math.asin(Math.sqrt(a));
        return rad * c;
    }
}
