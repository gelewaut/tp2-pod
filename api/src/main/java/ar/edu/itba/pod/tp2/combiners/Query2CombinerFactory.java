package ar.edu.itba.pod.tp2.combiners;

import ar.edu.itba.pod.tp2.models.FluxValue;
import ar.edu.itba.pod.tp2.models.Query2Value;
import com.hazelcast.mapreduce.Combiner;
import com.hazelcast.mapreduce.CombinerFactory;

public class Query2CombinerFactory implements CombinerFactory<String, Double, Query2Value> {
    @Override
    public Combiner<Double, Query2Value> newCombiner(String s) {
        return new Query2Combiner();
    }

    private class Query2Combiner extends Combiner<Double, Query2Value> {

        private double sum;
        private long count;

        @Override
        public void beginCombine() {
            sum = 0;
            count = 0;
        }

        @Override
        public void combine(Double aDouble) {
            sum += aDouble;
            count++;
        }

        @Override
        public Query2Value finalizeChunk() {
            return new Query2Value(sum, count);
        }

        @Override
        public void reset() {
            sum = 0;
            count = 0;
        }
    }
}
