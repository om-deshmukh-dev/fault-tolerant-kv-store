package framework.shardkv;

import framework.atmostonce.AMOCommand;
import framework.atmostonce.AMOResult;
import framework.Address;
import framework.Client;
import framework.Command;
import framework.Result;
import framework.kvstore.KVStore.SingleKeyCommand;
import framework.paxos.PaxosReply;
import framework.paxos.PaxosRequest;
import framework.shardmaster.ShardMaster.Query;
import framework.shardmaster.ShardMaster.ShardConfig;
import java.util.Set;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public class ShardStoreClient extends ShardStoreNode implements Client {
  private ShardConfig shardConfigLatest;
  private int sequenceNumCommands; // for uniquely identifying commands
  private int sequenceNumQueries; // for de-duplicating queries
  private Result result; // for getResult()

  /* -----------------------------------------------------------------------------------------------
   *  Construction and Initialization
   * ---------------------------------------------------------------------------------------------*/
  public ShardStoreClient(Address address, Address[] shardMasters, int numShards) {
    super(address, shardMasters, numShards);
    this.shardConfigLatest = null;
    this.sequenceNumCommands = 0;
    this.sequenceNumQueries = 0;
  }

  @Override
  public synchronized void init() {
    sendQueryShardMasters();
    set(new QueryTimer(), QueryTimer.QUERY_RETRY_MILLIS);
  }

  /* -----------------------------------------------------------------------------------------------
   *  Client Methods
   * ---------------------------------------------------------------------------------------------*/
  @Override
  public synchronized void sendCommand(Command command) {
    ShardStoreRequest request = new ShardStoreRequest(new AMOCommand(command, this.address(), this.sequenceNumCommands));
    this.result = null;

    assertWithThrow(this.shardConfigLatest == null);

    set(new ClientTimer(request), ClientTimer.CLIENT_RETRY_MILLIS);
  }

  @Override
  public synchronized boolean hasResult() { return this.result != null; }

  @Override
  public synchronized Result getResult() throws InterruptedException {
    while (this.result == null) {
      wait();
    }
    return this.result;
  }

  /* -----------------------------------------------------------------------------------------------
   *  Message Handlers
   * ---------------------------------------------------------------------------------------------*/

  // for SingleKeyCommands
  private synchronized void handleShardStoreReply(ShardStoreReply m, Address sender) {
    assertWithThrow(false);
  }


  // for queries
  private synchronized void handlePaxosReply(PaxosReply m, Address sender) {
    AMOResult amoResult = m.result();

    if (amoResult.sequenceNum() == this.sequenceNumQueries) {
      ShardConfig shardConfigNew = (ShardConfig) amoResult.result();
      assertWithThrow(this.shardConfigLatest == null || this.shardConfigLatest.configNum() <= shardConfigNew.configNum());
      this.shardConfigLatest = shardConfigNew;
      this.sequenceNumQueries++;
    }
  }

  /* -----------------------------------------------------------------------------------------------
   *  Timer Handlers
   * ---------------------------------------------------------------------------------------------*/
  private synchronized void onClientTimer(ClientTimer t) {
    // reset timer while no config yet
    if (this.shardConfigLatest == null) {
      set(t, ClientTimer.CLIENT_RETRY_MILLIS);
      return;
    }

    // reset timer for latest ongoing request
    if (t.request().command().sequenceNum() == this.sequenceNumCommands) {
      SingleKeyCommand command = (SingleKeyCommand) t.request().command().command();
      int groupIdManagingShard = getGroupIdForShard(keyToShard(command.key()));

      assertWithThrow(this.shardConfigLatest.groupInfo().containsKey(groupIdManagingShard));
      Set<Address> serversInGroup = this.shardConfigLatest.groupInfo().get(groupIdManagingShard).getLeft();

      assertWithThrow(!serversInGroup.isEmpty());
      broadcast(t.request(), serversInGroup);
    }
  }

  private synchronized void onQueryTimer(QueryTimer t) {
    sendQueryShardMasters();
    set(t, QueryTimer.QUERY_RETRY_MILLIS);
  }

  /* -----------------------------------------------------------------------------------------------
   *  Helpers
   * ---------------------------------------------------------------------------------------------*/

  // send a query with the latest configuration number to all shard masters
  private void sendQueryShardMasters() {
    Query query = new Query(-1);
    broadcast(
        new PaxosRequest(new AMOCommand(query, this.address(), this.sequenceNumQueries)),
        this.shardMasters()
    );
  }

  // Find the group in the current configuration managing `shardNum`.
  // It is required that exactly one group is managing this shard in the
  // latest configuration at the client.
  private int getGroupIdForShard(int shardNum) {
    assertWithThrow(this.shardConfigLatest != null);

    for (Integer groupId : this.shardConfigLatest.groupInfo().keySet()) {
      Pair<Set<Address>, Set<Integer>> groupMetadata = this.shardConfigLatest.groupInfo().get(groupId);

      if (groupMetadata.getRight().contains(shardNum)) { return groupId; }
    }

    // should never get to this point (the server set partitions the shards)
    assertWithThrow(false);
    return -1;
  }

  private void assertWithThrow(boolean b) {
    if (!b) {
      System.exit(1);
    }
  }
}
