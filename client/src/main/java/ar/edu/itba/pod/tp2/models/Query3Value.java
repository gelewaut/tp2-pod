package ar.edu.itba.pod.tp2.models;

import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.LocalTime;

public class Query3Value implements DataSerializable {
    private Long minutes;
    private LocalDateTime startDate;
    private Address address = new Address();

    public Query3Value() {}

    public Query3Value(long minutes, LocalDateTime startDate) {
        this.minutes = minutes;
        this.startDate = startDate;
    }

    public Long getMinutes() {
        return minutes;
    }

    public LocalDateTime getStartDate() {
        return startDate;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeLong(minutes);
        out.writeUTF(startDate.toString());
        address.writeData(out);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        minutes = in.readLong();
        startDate = LocalDateTime.parse(in.readUTF());
        address = new Address();
        address.readData(in); // since Address is DataSerializable let it read its own internal state
    }
}
