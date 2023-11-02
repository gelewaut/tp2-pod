package ar.edu.itba.pod.tp2.collators;

import com.hazelcast.mapreduce.Collator;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class Query2Collator implements Collator<Map.Entry<String, Double>, List<Map.Entry<String, Double>>> {
    @Override
    public List<Map.Entry<String, Double>> collate(Iterable<Map.Entry<String, Double>> values) {
        Stream<Map.Entry<String, Double>> stream = StreamSupport.stream(values.spliterator(), false);

        return stream.sorted((entry1, entry2) -> {
            int longComparison = Double.compare(entry2.getValue(), entry1.getValue());
            if (longComparison != 0) {
                return longComparison;
            } else {
                return entry1.getKey().compareTo(entry2.getKey());
            }
        }).toList();
    }
}
