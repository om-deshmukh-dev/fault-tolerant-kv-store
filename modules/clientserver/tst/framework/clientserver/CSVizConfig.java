package framework.clientserver;

import framework.Address;
import framework.testing.StateGenerator;
import framework.testing.StateGenerator.StateGeneratorBuilder;
import framework.testing.search.SearchState;
import framework.testing.visualization.VizConfig;
import framework.kvstore.KVStoreWorkload;
import java.util.List;

import static framework.clientserver.ClientServerBaseTest.SA;
import static framework.clientserver.ClientServerBaseTest.builder;

public class CSVizConfig extends VizConfig {
    @Override
    public SearchState getInitialState(int numServers, int numClients,
                                       List<List<String>> commands) {
        SearchState searchState =
                super.getInitialState(0, numClients, commands);
        searchState.addServer(SA);
        return searchState;
    }

    @Override
    protected StateGenerator stateGenerator(List<Address> servers,
                                            List<Address> clients,
                                            List<List<String>> workload) {
        StateGeneratorBuilder builder = builder();
        builder.workloadSupplier(a ->
            KVStoreWorkload.builder().commandStrings(workload.get(clients.indexOf(a))).build());
        return builder.build();
    }
}
