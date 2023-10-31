package ar.edu.itba.pod.tp2.reducers;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

public class Query2ReducerFactory implements ReducerFactory<String, Double, Double> {
    @Override
    public Reducer<Double, Double> newReducer(String s) {
        return new Query2Reducer();
    }

    private class Query2Reducer extends Reducer<Double , Double> {
        private double sum;
        private long count;


        @Override
        public void beginReduce() {
            sum = 0;
            count = 0;
        }

        @Override
        public void reduce(Double aDouble) {
            sum += aDouble;
            count += 1;
        }

        @Override
        public Double finalizeReduce() {
            return sum/count;
        }
    }
}
