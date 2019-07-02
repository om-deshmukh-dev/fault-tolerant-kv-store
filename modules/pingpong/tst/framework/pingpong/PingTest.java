package framework.pingpong;

import framework.Address;
import framework.Command;
import framework.Result;
import framework.testing.LocalAddress;
import framework.testing.StateGenerator;
import framework.testing.StateGenerator.StateGeneratorBuilder;
import framework.testing.Workload;
import framework.testing.junit.BaseJUnitTest;
import framework.testing.junit.PrettyTestName;
import framework.testing.junit.RunTests;
import framework.testing.junit.SearchTests;
import framework.testing.junit.UnreliableTests;
import framework.testing.runner.RunState;
import framework.testing.search.Search;
import framework.testing.search.SearchState;
import framework.testing.utils.SerializableFunction;
import framework.pingpong.PingApplication.Ping;
import framework.pingpong.PingApplication.Pong;
import java.util.Objects;
import lombok.NonNull;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runners.MethodSorters;

import static framework.testing.StatePredicate.CLIENTS_DONE;
import static framework.testing.StatePredicate.RESULTS_OK;
import static framework.testing.search.SearchResults.EndCondition.INVARIANT_VIOLATED;
import static framework.testing.search.SearchResults.EndCondition.SPACE_EXHAUSTED;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public final class PingTest extends BaseJUnitTest {
    static final Address sa = new LocalAddress("pingserver");

    private static final class PingParser implements
            SerializableFunction<Pair<String, String>, Pair<Command, Result>> {
        @Override
        public Pair<Command, Result> apply(
                @NonNull Pair<String, String> commandAndResultString) {
            return new ImmutablePair<>(
                    new Ping(commandAndResultString.getValue()),
                    new Pong(commandAndResultString.getValue()));
        }
    }

    static StateGeneratorBuilder builder() {
        StateGeneratorBuilder builder = StateGenerator.builder();
        builder.serverSupplier(a -> {
            if (!Objects.equals(a, sa)) {
                throw new IllegalArgumentException();
            }
            return new PingServer(sa);
        });
        builder.clientSupplier(a -> new PingClient(a, sa));
        builder.workloadSupplier(Workload.emptyWorkload());

        return builder;
    }

    @Before
    public void setup() {
        builder = builder();

        runState = new RunState(builder.build());
        runState.addServer(sa);

        initSearchState = new SearchState(builder.build());
        initSearchState.addServer(sa);
    }

    static Workload repeatedPings(int numPings) {
        return Workload.builder().parser(new PingParser())
                       .commandStrings("ping-%i").resultStrings("ping-%i")
                       .numTimes(numPings).build();
    }

    @Test(timeout = 5 * 1000)
    @PrettyTestName("Single client ping test")
    @Category({RunTests.class})
    public void test01BasicPing() throws InterruptedException {
        Workload workload =
                Workload.builder().commands(new Ping("Hello, World!"))
                        .results(new Pong("Hello, World!")).build();
        runState.addClientWorker(client(1), workload);

        runSettings.addInvariant(RESULTS_OK);
        runState.run(runSettings);
    }

    @Test(timeout = 5 * 1000)
    @PrettyTestName("Multiple clients can ping simultaneously")
    @Category({RunTests.class})
    public void test02MultipleClientsPing() throws InterruptedException {
        Workload workload = Workload.builder().parser(new PingParser())
                                    .commandStrings("hello from %a")
                                    .resultStrings("hello from %a").build();

        for (int i = 1; i <= 10; i++) {
            runState.addClientWorker(client(i), workload);
        }

        runSettings.addInvariant(RESULTS_OK);
        runState.run(runSettings);
    }

    @Test(timeout = 5 * 1000)
    @PrettyTestName("Client can still ping if some messages are dropped")
    @Category({RunTests.class, UnreliableTests.class})
    public void test03MessagesDropped() throws InterruptedException {
        runState.addClientWorker(client(1), repeatedPings(100));

        runSettings.networkUnreliable(true);

        runSettings.addInvariant(RESULTS_OK);
        runState.run(runSettings);
    }

    @Test
    @PrettyTestName("Single client repeatedly pings")
    @Category(SearchTests.class)
    public void test04PingSearch() throws InterruptedException {
        initSearchState.addClientWorker(client(1), repeatedPings(10));

        System.out.println("Checking that the client can finish all pings");
        searchSettings.addInvariant(CLIENTS_DONE.negate()).maxTimeSecs(10);
        assertEndConditionAndContinue(INVARIANT_VIOLATED,
                Search.bfs(initSearchState, searchSettings));

        System.out
                .println("Checking that all of the returned pongs match pings");
        searchSettings.clearInvariants().addPrune(CLIENTS_DONE)
                      .addInvariant(RESULTS_OK);
        assertEndCondition(SPACE_EXHAUSTED,
                Search.bfs(initSearchState, searchSettings));
    }
}
