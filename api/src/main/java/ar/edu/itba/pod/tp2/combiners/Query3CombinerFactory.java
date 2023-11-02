package ar.edu.itba.pod.tp2.combiners;

import ar.edu.itba.pod.tp2.models.Query3Value;
import ar.edu.itba.pod.tp2.models.Ride;
import com.google.common.util.concurrent.ClosingFuture;
import com.hazelcast.mapreduce.Combiner;
import com.hazelcast.mapreduce.CombinerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;

public class Query3CombinerFactory implements CombinerFactory<String, Query3Value, Query3Value> {

    private static final Logger logger = LoggerFactory.getLogger(Query3CombinerFactory.class);


    @Override
    public Combiner<Query3Value, Query3Value> newCombiner(String key) {
        return new Query3Combiner();
    }

    private class Query3Combiner extends Combiner<Query3Value, Query3Value> {
        private long longestTrip;
        private LocalDateTime longestTripDate;
        private String endStation;


        @Override
        public void beginCombine() {
            this.longestTrip = 0;
            this.longestTripDate = null;
            this.endStation = null;
        }

        @Override
        public void combine(Query3Value value) {
            if (longestTripDate == null) {
                longestTripDate = value.getStartDate();
                longestTrip = value.getMinutes();
                endStation = value.getEndStation();
            } else if (value.getMinutes() > this.longestTrip) {
                this.longestTrip = value.getMinutes();
                this.longestTripDate = value.getStartDate();
                this.endStation = value.getEndStation();
            } else if (value.getMinutes() == this.longestTrip) {
                if (value.getStartDate() != null && longestTripDate.isAfter(value.getStartDate())) {
                    longestTripDate = value.getStartDate();
                    endStation = value.getEndStation();
                }
            }
        }

        @Override
        public Query3Value finalizeChunk() {
            return new Query3Value(longestTrip, longestTripDate, endStation);
        }

        @Override
        public void reset() {
            this.longestTrip = 0;
            this.longestTripDate = null;
            this.endStation = null;
        }
    }
}