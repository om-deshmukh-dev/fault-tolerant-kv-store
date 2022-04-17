package framework.paxos;

import framework.Address;
import framework.testing.StateGenerator;
import framework.testing.StateGenerator.StateGeneratorBuilder;
import framework.testing.junit.Lab;
import framework.testing.visualization.VizConfig;
import framework.kvstore.KVStoreWorkload;
import java.util.List;

import static framework.paxos.PaxosTest.builder;

@Lab("3")
public class PaxosVizConfig extends VizConfig {
    @Override
    protected StateGenerator stateGenerator(List<Address> servers,
                                            List<Address> clients,
                                            List<List<String>> workload) {
        final Address[] serverAddresses = servers.toArray(new Address[0]);
        StateGeneratorBuilder builder = builder(serverAddresses);
        builder.workloadSupplier(a ->
            KVStoreWorkload.builder().commandStrings(workload.get(clients.indexOf(a))).build());
        return builder.build();
    }
}
