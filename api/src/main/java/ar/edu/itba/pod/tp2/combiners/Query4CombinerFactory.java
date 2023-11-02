package ar.edu.itba.pod.tp2.combiners;

import ar.edu.itba.pod.tp2.models.FluxValue;
import com.hazelcast.mapreduce.Combiner;
import com.hazelcast.mapreduce.CombinerFactory;

public class Query4CombinerFactory implements CombinerFactory<String, Long, FluxValue> {
    @Override
    public Combiner<Long, FluxValue> newCombiner(String s) {
        return new Query4Combiner();
    }
    private class Query4Combiner extends Combiner<Long, FluxValue> {
        private long positive;
        private long neutral;
        private long negative;

        @Override
        public void beginCombine() {
            positive = 0;
            neutral = 0;
            negative = 0;
        }

        @Override
        public void combine(Long value) {
            if (value > 0) {
                positive++;
            } else if (value < 0) {
                negative++;
            } else {
                neutral++;
            }
        }
        @Override
        public FluxValue finalizeChunk() {
            return new FluxValue(positive, neutral, negative);
        }
        @Override
        public void reset() {
            positive = 0;
            neutral = 0;
            negative = 0;
        }
    }
}
