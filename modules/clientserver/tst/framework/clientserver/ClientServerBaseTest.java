package framework.clientserver;

import framework.Address;
import framework.testing.LocalAddress;
import framework.testing.StateGenerator;
import framework.testing.StateGenerator.StateGeneratorBuilder;
import framework.testing.junit.BaseJUnitTest;
import framework.testing.runner.RunState;
import framework.testing.search.SearchState;
import framework.kvstore.KVStore;
import framework.kvstore.KVStoreWorkload;
import java.util.Objects;

abstract class ClientServerBaseTest extends BaseJUnitTest {
    static final Address SA = new LocalAddress("server");

    static StateGeneratorBuilder builder() {
        StateGeneratorBuilder builder = StateGenerator.builder();
        builder.serverSupplier(a -> {
            if (!Objects.equals(a, SA)) {
                throw new IllegalArgumentException();
            }
            return new SimpleServer(SA, new KVStore());
        });
        builder.clientSupplier(a -> new SimpleClient(a, SA));
        builder.workloadSupplier(KVStoreWorkload.emptyWorkload());
        return builder;
    }

    @Override
    protected void setupRunTest() {
        runState = new RunState(builder().build());
        runState.addServer(SA);
    }

    @Override
    protected void setupSearchTest() {
        initSearchState = new SearchState(builder().build());
        initSearchState.addServer(SA);
    }
}

