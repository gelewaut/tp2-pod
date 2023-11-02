package ar.edu.itba.pod.tp2.reducers;

import ar.edu.itba.pod.tp2.models.FluxValue;
import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Query4ReducerCombinerFactory implements ReducerFactory<String, FluxValue, FluxValue> {

    private static final Logger logger = LoggerFactory.getLogger(Query4ReducerCombinerFactory.class);
    @Override
    public Reducer<FluxValue, FluxValue> newReducer(String s) {
        return new Query4ReducerCombiner();
    }

    private class Query4ReducerCombiner extends Reducer<FluxValue, FluxValue> {
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
        public void reduce(FluxValue value) {
            positive+= value.getPositive();
            negative+= value.getNegative();
            neutral+= value.getNeutral();
        }

        @Override
        public FluxValue finalizeReduce() {
            return new FluxValue(positive, neutral, negative);
        }
    }
}
