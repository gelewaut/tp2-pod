package ar.edu.itba.pod.tp2.server;

import com.hazelcast.config.*;
import com.hazelcast.core.Hazelcast;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Properties;

public class Server {
    private static final Logger logger = LoggerFactory.getLogger(Server.class);
    public static void main(String[] args) {logger.info("hz-config Server Starting ...");

        Properties props = System.getProperties();
        String address = props.getProperty("address", "192.168.0.*");

        // Config
        Config config = new Config();

        // Group Config
        GroupConfig groupConfig = new GroupConfig().setName("G9").setPassword("G09-pass");
        config.setGroupConfig(groupConfig);

        // Network Config
        MulticastConfig multicastConfig = new MulticastConfig().setEnabled(true).setMulticastGroup("224.2.2.3").setMulticastPort(54327);
        JoinConfig joinConfig = new JoinConfig().setMulticastConfig(multicastConfig);
        InterfacesConfig interfacesConfig = new InterfacesConfig().setInterfaces(Collections.singletonList(address)).setEnabled(true);
        NetworkConfig networkConfig = new NetworkConfig().setInterfaces(interfacesConfig).setJoin(joinConfig).setPortAutoIncrement(true);

        config.setNetworkConfig(networkConfig);

        // Management Center Config
        ManagementCenterConfig managementCenterConfig = new ManagementCenterConfig()
                .setUrl("http://localhost:8080/mancenter_3_8_5/").setEnabled(true);
        if (props.getProperty("management") != null) {
            config.setManagementCenterConfig(managementCenterConfig);
        }

        //  Start cluster
        Hazelcast.newHazelcastInstance(config);
    }
}
