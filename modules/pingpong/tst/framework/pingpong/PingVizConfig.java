package framework.pingpong;

import framework.testing.StateGenerator;
import framework.testing.StateGenerator.StateGeneratorBuilder;
import framework.testing.Workload;
import framework.testing.search.SearchState;
import framework.testing.visualization.VizConfig;
import framework.pingpong.PingApplication.Ping;
import java.util.List;
import java.util.stream.Collectors;

import static framework.pingpong.PingTest.builder;
import static framework.pingpong.PingTest.sa;

public class PingVizConfig extends VizConfig {
    @Override
    public SearchState getInitialState(int numServers, int numClients,
                                       List<String> commands) {
        SearchState searchState =
                super.getInitialState(0, numClients, commands);
        searchState.addServer(sa);
        return searchState;
    }

    @Override
    protected StateGenerator stateGenerator(List<String> workload) {
        StateGeneratorBuilder builder = builder();
        builder.workloadSupplier(__ -> Workload.workload(
                workload.stream().map(Ping::new).collect(Collectors.toList())));
        return builder.build();
    }
}
