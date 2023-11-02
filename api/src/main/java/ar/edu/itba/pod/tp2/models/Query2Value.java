package ar.edu.itba.pod.tp2.models;

import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;

public class Query2Value implements DataSerializable {

    private double sum;
    private long count;
    private Address address = new Address();

    public Query2Value(){}
    public Query2Value(double sum, long count) {
        this.sum = sum;
        this.count = count;
    }

    public double getSum() {
        return sum;
    }

    public long getCount() {
        return count;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeDouble(sum);
        out.writeLong(count);
        address.writeData(out);
    }

    public void readData(ObjectDataInput in) throws IOException {
        sum = in.readDouble();
        count = in.readLong();
        address = new Address();
        address.readData(in);
    }
}
