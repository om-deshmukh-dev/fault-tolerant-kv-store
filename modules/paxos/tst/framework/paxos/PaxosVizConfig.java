package framework.paxos;

import framework.Address;
import framework.testing.StateGenerator;
import framework.testing.StateGenerator.StateGeneratorBuilder;
import framework.testing.visualization.VizConfig;
import framework.kvstore.KVStoreWorkload;
import java.util.List;

import static framework.paxos.PaxosTest.builder;

public class PaxosVizConfig extends VizConfig {
    @Override
    protected StateGenerator stateGenerator(List<Address> servers,
                                            List<Address> clients,
                                            List<String> commands) {
        final Address[] serverAddresses = servers.toArray(new Address[0]);
        StateGeneratorBuilder builder = builder(serverAddresses);
        builder.workloadSupplier(
                KVStoreWorkload.builder().commandStrings(commands).build());
        return builder.build();
    }
}
