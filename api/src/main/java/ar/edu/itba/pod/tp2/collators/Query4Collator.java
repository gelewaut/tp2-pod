package ar.edu.itba.pod.tp2.collators;

import ar.edu.itba.pod.tp2.models.FluxValue;
import com.hazelcast.mapreduce.Collator;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class Query4Collator implements Collator<Map.Entry<String, FluxValue>, List<Map.Entry<String, FluxValue>>> {
    @Override
    public List<Map.Entry<String, FluxValue>> collate(Iterable<Map.Entry<String, FluxValue>> values) {
        Stream<Map.Entry<String, FluxValue>> stream = StreamSupport.stream(values.spliterator(), false);

        return stream.sorted((entry1, entry2) -> {
            int aux = Long.compare(entry2.getValue().getPositive(), entry1.getValue().getPositive());
            if (aux == 0) {
                return entry1.getKey().compareTo(entry2.getKey());
            }
            return aux;
        }).toList();
    }
}