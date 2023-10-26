package ar.edu.itba.pod.tp2.models;

import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;
import java.time.LocalDateTime;

public class Ride implements DataSerializable {
    private LocalDateTime startDate;
    private int startPk;
    private LocalDateTime endDate;
    private int endPk;
    private boolean isMember;
    private Address address = new Address();

    public Ride (
            LocalDateTime startDate,
            int startPk,
            LocalDateTime endDate,
            int endPk,
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
        out.writeInt(startPk);
        out.writeUTF(endDate.toString());
        out.writeInt(endPk);
        out.writeBoolean(isMember);
        address.writeData (out);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        startDate = LocalDateTime.parse(in.readUTF());
        startPk = in.readInt();
        endDate = LocalDateTime.parse(in.readUTF());
        endPk = in.readInt();
        isMember = in.readBoolean();
        address = new Address();
        address.readData(in); // since Address is DataSerializable let it read its own internal state
    }

    public int getEndPk() {
        return endPk;
    }

    public int getStartPk() {
        return startPk;
    }

    public LocalDateTime getEndDate() {
        return endDate;
    }

    public LocalDateTime getStartDate() {
        return startDate;
    }
}
