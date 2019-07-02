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
    static final Address sa = new LocalAddress("server");

    @Override
    protected void setupTest() {
        builder = builder();

        runState = new RunState(builder.build());
        runState.addServer(sa);

        initSearchState = new SearchState(builder.build());
        initSearchState.addServer(sa);
    }

    protected static StateGeneratorBuilder builder() {
        StateGeneratorBuilder builder = StateGenerator.builder();
        builder.serverSupplier(a -> {
            if (!Objects.equals(a, sa)) {
                throw new IllegalArgumentException();
            }
            return new SimpleServer(sa, new KVStore());
        });
        builder.clientSupplier(a -> new SimpleClient(a, sa));
        builder.workloadSupplier(KVStoreWorkload.emptyWorkload());

        return builder;
    }
}

