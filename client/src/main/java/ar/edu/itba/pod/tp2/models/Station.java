package ar.edu.itba.pod.tp2.models;

import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;

public class Station implements DataSerializable {
    private int pk;
    private String name;
    private double latitude;
    private double longitude;

    private Address address = new Address();

    public Station (
            int pk,
            String name,
            double latitude,
            double longitude) {
        this.pk = pk;
        this.name = name;
        this.latitude = latitude;
        this.longitude = longitude;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(pk);
        out.writeUTF(name);
        out.writeDouble(latitude);
        out.writeDouble(longitude);
        address.writeData(out);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        pk = in.readInt();
        name = in.readUTF();
        latitude = in.readDouble();
        longitude = in.readDouble();
        address = new Address();
        address.readData(in); // since Address is DataSerializable let it read its own internal state
    }

    public int getPk() {
        return pk;
    }
}
