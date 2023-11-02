package ar.edu.itba.pod.tp2.reducers;

import ar.edu.itba.pod.tp2.models.FluxValue;
import ar.edu.itba.pod.tp2.models.Query2Value;
import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Query2ReducerCombinerFactory implements ReducerFactory<String, Query2Value, Double> {

    private static final Logger logger = LoggerFactory.getLogger(Query2ReducerCombinerFactory.class);
    @Override
    public Reducer<Query2Value, Double> newReducer(String s) {
        return new Query2ReducerCombiner();
    }

    private class Query2ReducerCombiner extends Reducer<Query2Value, Double> {

        double sum;
        long count;

        @Override
        public void beginReduce() {
            sum = 0;
            count = 0;
        }

        @Override
        public void reduce(Query2Value query2Value) {
            sum += query2Value.getSum();
            count += query2Value.getCount();
        }

        @Override
        public Double finalizeReduce() {
            return sum/count;
        }
    }
}
