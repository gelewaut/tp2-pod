package ar.edu.itba.pod.tp2.combiners;

import com.hazelcast.mapreduce.Combiner;
import com.hazelcast.mapreduce.CombinerFactory;
import com.hazelcast.mapreduce.Reducer;

public class Query1CombinerFactory implements CombinerFactory<String, Long, Long> {
    @Override
    public Combiner<Long, Long> newCombiner(String s) {
        return new Query1Combiner();
    }

    private class Query1Combiner extends Combiner<Long, Long>{
        private long sum;

        @Override
        public void beginCombine() {
            sum = 0;
        }

        @Override
        public void combine(Long aLong) {
            sum += aLong;
        }

        @Override
        public Long finalizeChunk() {
            return sum;
        }

        @Override
        public void reset(){
            sum = 0;
        }
    }
}
