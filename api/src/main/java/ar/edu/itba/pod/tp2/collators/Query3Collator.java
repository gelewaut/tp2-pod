package ar.edu.itba.pod.tp2.collators;

import ar.edu.itba.pod.tp2.models.Query3Value;
import com.hazelcast.mapreduce.Collator;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class Query3Collator implements Collator<Map.Entry<String, Query3Value>, List<Map.Entry<String, Query3Value>>> {

    @Override
    public List<Map.Entry<String, Query3Value>> collate(Iterable<Map.Entry<String, Query3Value>> values) {
        Stream<Map.Entry<String, Query3Value>> stream = StreamSupport.stream(values.spliterator(), false);

        return stream.sorted((entry1, entry2) -> {
            int aux = entry2.getValue().getMinutes().compareTo(entry1.getValue().getMinutes());
            if (aux == 0) {
                return entry1.getKey().compareTo(entry2.getKey());
            }
            return aux;
        }).toList();
    }
}