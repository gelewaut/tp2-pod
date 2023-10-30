package ar.edu.itba.pod.tp2.models;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;
import java.time.LocalDateTime;

public class Ride implements DataSerializable{
    private LocalDateTime startDate;
    private long startPk;
    private LocalDateTime endDate;
    private long endPk;
    private boolean isMember;
    private Address address = new Address();

    public Ride(){};

    public Ride (
            LocalDateTime startDate,
            long startPk,
            LocalDateTime endDate,
            long endPk,
            boolean isMember) {
        this.startDate = startDate;
        this.startPk = startPk;
        this.endDate = endDate;
        this.endPk = endPk;
        this.isMember = isMember;
    }
    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(startDate.toString());
        out.writeLong(startPk);
        out.writeUTF(endDate.toString());
        out.writeLong(endPk);
        out.writeBoolean(isMember);
        address.writeData (out);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        startDate = LocalDateTime.parse(in.readUTF());
        startPk = in.readLong();
        endDate = LocalDateTime.parse(in.readUTF());
        endPk = in.readLong();
        isMember = in.readBoolean();
        address = new Address();
        address.readData(in); // since Address is DataSerializable let it read its own internal state
    }

    public long getEndPk() {
        return endPk;
    }

    public long getStartPk() {
        return startPk;
    }

    public LocalDateTime getEndDate() {
        return endDate;
    }

    public LocalDateTime getStartDate() {
        return startDate;
    }
}
