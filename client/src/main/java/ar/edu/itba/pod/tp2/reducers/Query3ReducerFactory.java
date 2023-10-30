package ar.edu.itba.pod.tp2.reducers;

import ar.edu.itba.pod.tp2.models.Query3Value;
import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

import java.time.LocalDateTime;

public class Query3ReducerFactory implements ReducerFactory<String, Query3Value, Query3Value> {
    @Override
    public Reducer<Query3Value, Query3Value> newReducer(String s) {
        return new Query3Reducer();
    }

    private class Query3Reducer extends Reducer<Query3Value, Query3Value> {
        private long longestTrip;
        private LocalDateTime longestTripDate;

        @Override
        public void beginReduce() {
            longestTrip = 0;
        }

        @Override
        public void reduce(Query3Value value) {
            if (value.getMinutes() > this.longestTrip) {
                this.longestTrip = value.getMinutes();
                this.longestTripDate = value.getStartDate();
            }
            else if (value.getMinutes() == this.longestTrip) {
                if (longestTripDate.isAfter(value.getStartDate())) {
                    longestTripDate = value.getStartDate();
                }
            }
        }

        @Override
        public Query3Value finalizeReduce() {
            return new Query3Value(longestTrip, longestTripDate);
        }
    }
}
