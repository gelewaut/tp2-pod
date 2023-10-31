package ar.edu.itba.pod.tp2.reducers;

import ar.edu.itba.pod.tp2.models.FluxValue;
import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Query4ReducerFactory implements ReducerFactory<String, Long, FluxValue> {

    private static final Logger logger = LoggerFactory.getLogger(Query4ReducerFactory.class);
    @Override
    public Reducer<Long, FluxValue> newReducer(String s) {
        return new Query4Reducer();
    }

    private class Query4Reducer extends Reducer<Long, FluxValue> {
        private long positive;
        private long neutral;
        private long negative;

        @Override
        public void beginReduce() {
            positive = 0;
            neutral = 0;
            negative = 0;
        }

        @Override
        public void reduce(Long value) {
            if (value > 0) {
                positive++;
            } else if (value < 0) {
                negative++;
            } else {
                neutral++;
            }
        }

        @Override
        public FluxValue finalizeReduce() {
            return new FluxValue(positive, neutral, negative);
        }
    }
}