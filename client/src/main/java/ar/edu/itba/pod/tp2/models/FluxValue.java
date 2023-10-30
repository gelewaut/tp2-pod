package ar.edu.itba.pod.tp2.models;

import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;

public class FluxValue implements DataSerializable {

    private long positive;
    private long neutral;
    private long negative;
    private Address address = new Address();

    public FluxValue() {};
    public FluxValue(long positive, long neutral, long negative) {
        this.positive = positive;
        this.neutral = neutral;
        this.negative = negative;
    }

    @Override
    public String toString() {
        return positive + ";"
                + neutral + ";"
                + negative;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeLong(positive);
        out.writeLong(neutral);
        out.writeLong(negative);
        address.writeData(out);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        positive = in.readLong();
        neutral = in.readLong();
        negative = in.readLong();
        address = new Address();
        address.readData(in); // since Address is DataSerializable let it read its own internal state
    }
}
