package ar.edu.itba.pod.tp2.collators;

import com.hazelcast.mapreduce.Collator;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class Query1Collator implements Collator<Map.Entry<String, Long>, List<Map.Entry<String, Long>>> {
    @Override
    public List<Map.Entry<String, Long>> collate(Iterable<Map.Entry<String, Long>> values) {
        Stream<Map.Entry<String, Long>> stream = StreamSupport.stream(values.spliterator(), false);

        return stream.sorted((entry1, entry2) -> {
            int longComparison = Long.compare(entry2.getValue(), entry1.getValue());
            if (longComparison != 0) {
                return longComparison;
            } else {
                return entry1.getKey().compareTo(entry2.getKey());
            }
        }).toList();
    }
}
