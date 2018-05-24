package framework.primarybackup;

import framework.Address;
import framework.testing.LocalAddress;
import framework.testing.StateGenerator;
import framework.testing.StateGenerator.StateGeneratorBuilder;
import framework.testing.search.SearchState;
import framework.testing.visualization.VizConfig;
import framework.kvstore.KVStoreWorkload;
import java.util.List;

import static framework.primarybackup.PrimaryBackupTest.builder;

public class PBVizConfig extends VizConfig {
    private static final Address vsa = new LocalAddress("viewserver");

    @Override
    public SearchState getInitialState(int numServers, int numClients,
                                       List<String> commands) {
        SearchState searchState =
                super.getInitialState(numServers, numClients, commands);
        searchState.addServer(vsa);
        return searchState;
    }

    @Override
    protected StateGenerator stateGenerator(List<String> commands) {
        StateGeneratorBuilder builder = builder();
        builder.workloadSupplier(
                KVStoreWorkload.builder().commandStrings(commands).build());
        return builder.build();
    }
}

