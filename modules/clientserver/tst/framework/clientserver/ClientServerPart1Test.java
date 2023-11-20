package framework.clientserver;

import static framework.testing.StatePredicate.RESULTS_OK;
import static framework.kvstore.KVStoreWorkload.APPENDS_LINEARIZABLE;
import static framework.kvstore.KVStoreWorkload.appendDifferentKeyWorkload;
import static framework.kvstore.KVStoreWorkload.appendSameKeyWorkload;
import static framework.kvstore.KVStoreWorkload.get;
import static framework.kvstore.KVStoreWorkload.simpleWorkload;

import framework.Client;
import framework.testing.junit.Lab;
import framework.testing.junit.Part;
import framework.testing.junit.RunTests;
import framework.testing.junit.TestDescription;
import framework.testing.junit.TestPointValue;
import framework.testing.junit.UnreliableTests;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Lab("1")
@Part(2)
public final class ClientServerPart1Test extends ClientServerBaseTest {

  @Test(timeout = 2 * 1000, expected = InterruptedException.class)
  @TestDescription("Client throws InterruptedException")
  @Category(RunTests.class)
  @TestPointValue(5)
  public void test01ThrowsException() throws InterruptedException {
    final Thread mainThread = Thread.currentThread();
    Client client = runState.addClient(client(1));
    startThread(
        () -> {
          try {
            Thread.sleep(500);
          } catch (InterruptedException e) {
            return;
          }

          mainThread.interrupt();
        });
    client.sendCommand(get("FOO"));
    // Should never return since the runState wasn't started
    client.getResult();
  }

  @Test(timeout = 10 * 1000)
  @TestDescription("Single client basic operations")
  @Category(RunTests.class)
  @TestPointValue(20)
  public void test02SingleClient() throws InterruptedException {
    runState.addClientWorker(client(1), simpleWorkload);
    runSettings.addInvariant(RESULTS_OK);
    runState.run(runSettings);
  }

  @Test(timeout = 10 * 1000)
  @TestDescription("Multi-client different key appends")
  @Category(RunTests.class)
  @TestPointValue(20)
  public void test03MultiClient() throws InterruptedException {
    int numRounds = 100, numClients = 10;

    for (int i = 1; i <= numClients; i++) {
      runState.addClientWorker(client(i), appendDifferentKeyWorkload(numRounds));
    }

    runSettings.addInvariant(RESULTS_OK);
    runState.run(runSettings);
  }

  @Test(timeout = 10 * 1000)
  @TestDescription("Multi-client same key appends")
  @Category(RunTests.class)
  @TestPointValue(30)
  public void test04MultiClientAppends() throws InterruptedException {
    int numRounds = 5, numClients = 10;

    for (int i = 1; i <= numClients; i++) {
      runState.addClientWorker(client(i), appendSameKeyWorkload(numRounds));
    }

    runSettings.addInvariant(APPENDS_LINEARIZABLE);
    runState.run(runSettings);
  }

  @Test(timeout = 30 * 1000)
  @TestDescription("Single client can finish operations")
  @Category({RunTests.class, UnreliableTests.class})
  @TestPointValue(20)
  public void test05SingleClientFinishesUnreliable() throws InterruptedException {
    int numRounds = 25;

    runState.addClientWorker(client(1), appendDifferentKeyWorkload(numRounds));
    runSettings.networkUnreliable(true);

    runState.run(runSettings);
  }
}
