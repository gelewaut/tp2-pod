package ar.edu.itba.pod.tp2.server;

import com.hazelcast.config.*;
import com.hazelcast.core.Hazelcast;
import io.grpc.ServerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;

public class Server {
    private static final Logger logger = LoggerFactory.getLogger(Server.class);
    public static void main(String[] args) {logger.info("hz-config Server Starting ...");

        // Config
        Config config = new Config();

        // Group Config
        GroupConfig groupConfig = new GroupConfig().setName("G9").setPassword("G09-pass");
        config.setGroupConfig(groupConfig);

        // Network Config
        MulticastConfig multicastConfig = new MulticastConfig();
        JoinConfig joinConfig = new JoinConfig().setMulticastConfig(multicastConfig);
        InterfacesConfig interfacesConfig = new InterfacesConfig().setInterfaces(Collections.singletonList("192.168.0.151")).setEnabled(true);
//        InterfacesConfig interfacesConfig = new InterfacesConfig().setInterfaces(Collections.singletonList("127.0.0.*")).setEnabled(true);
        NetworkConfig networkConfig = new NetworkConfig().setInterfaces(interfacesConfig).setJoin(joinConfig);
        config.setNetworkConfig(networkConfig);

        // Management Center Config
        ManagementCenterConfig managementCenterConfig = new ManagementCenterConfig()
                .setUrl("http://localhost:8080/mancenter_3_8_5/").setEnabled(true);
//        config.setManagementCenterConfig(managementCenterConfig);

        //  Start cluster
        Hazelcast.newHazelcastInstance(config);
    }
}
