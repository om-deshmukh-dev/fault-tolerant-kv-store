package framework.primarybackup;

import static framework.primarybackup.PrimaryBackupTest.builder;
import static framework.primarybackup.ViewServerTest.VSA;

import framework.Address;
import framework.testing.StateGenerator;
import framework.testing.StateGenerator.StateGeneratorBuilder;
import framework.testing.junit.Lab;
import framework.testing.search.SearchState;
import framework.testing.visualization.VizConfig;
import framework.kvstore.KVStoreWorkload;
import java.util.List;

@Lab("2")
public class PBVizConfig extends VizConfig {
  @Override
  public SearchState getInitialState(int numServers, int numClients, List<List<String>> commands) {
    SearchState searchState = super.getInitialState(numServers, numClients, commands);
    searchState.addServer(VSA);
    return searchState;
  }

  @Override
  protected StateGenerator stateGenerator(
      List<Address> servers, List<Address> clients, List<List<String>> workload) {
    StateGeneratorBuilder builder = builder();
    builder.workloadSupplier(
        a -> KVStoreWorkload.builder().commandStrings(workload.get(clients.indexOf(a))).build());
    return builder.build();
  }
}
