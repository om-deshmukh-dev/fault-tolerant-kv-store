package framework.pingpong;

import static framework.pingpong.PingTest.builder;
import static framework.pingpong.PingTest.sa;

import framework.Address;
import framework.testing.StateGenerator;
import framework.testing.StateGenerator.StateGeneratorBuilder;
import framework.testing.Workload;
import framework.testing.junit.Lab;
import framework.testing.search.SearchState;
import framework.testing.visualization.VizConfig;
import framework.pingpong.PingApplication.Ping;
import java.util.List;
import java.util.stream.Collectors;

@Lab("0")
public class PingVizConfig extends VizConfig {
  @Override
  public SearchState getInitialState(int numServers, int numClients, List<List<String>> commands) {
    SearchState searchState = super.getInitialState(0, numClients, commands);
    searchState.addServer(sa);
    return searchState;
  }

  @Override
  protected StateGenerator stateGenerator(
      List<Address> servers, List<Address> clients, List<List<String>> workload) {
    StateGeneratorBuilder builder = builder();
    builder.workloadSupplier(
        a ->
            Workload.workload(
                workload.get(clients.indexOf(a)).stream()
                    .map(Ping::new)
                    .collect(Collectors.toList())));
    return builder.build();
  }
}
