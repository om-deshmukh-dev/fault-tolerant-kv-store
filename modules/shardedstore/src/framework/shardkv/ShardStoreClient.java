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
    SingleKeyCommand singleKeyCommand = (SingleKeyCommand) command;
    this.result = null;

    if (this.shardConfigLatest != null) {
      sendRequestToGroupMembers(request, getGroupIdForShard(keyToShard(singleKeyCommand.key())));
    }

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
    AMOResult amoResult = m.result();

    if (amoResult.sequenceNum() == this.sequenceNumCommands) {
      this.result = amoResult.result();
      this.sequenceNumCommands++;
      notify();
    }
  }


  // for queries
  private synchronized void handlePaxosReply(PaxosReply m, Address sender) {
    AMOResult amoResult = m.result();

    if (amoResult.sequenceNum() == this.sequenceNumQueries) {
      ShardConfig shardConfigNew = (ShardConfig) amoResult.result();
      assertWithMessage(this.shardConfigLatest == null || this.shardConfigLatest.configNum() <= shardConfigNew.configNum(),
                        "ShardClient.handlePaxosReply: new config must be at least as large as this client's latest config");
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

    // reset timer and broadcast request again for latest ongoing request
    if (t.request().command().sequenceNum() == this.sequenceNumCommands) {
      SingleKeyCommand command = (SingleKeyCommand) t.request().command().command();
      int groupIdManagingShard = getGroupIdForShard(keyToShard(command.key()));

      sendRequestToGroupMembers(t.request(), groupIdManagingShard);
      set(t, ClientTimer.CLIENT_RETRY_MILLIS);
    }
  }

  private synchronized void onQueryTimer(QueryTimer t) {
    sendQueryShardMasters();
    set(t, QueryTimer.QUERY_RETRY_MILLIS);
  }

  /* -----------------------------------------------------------------------------------------------
   *  Helpers
   * ---------------------------------------------------------------------------------------------*/

  // send request to all group members associated with the group ID given as argument.
  // requires that the latest configuration is not null
  private void sendRequestToGroupMembers(ShardStoreRequest request, int groupIdManagingShard) {
    assertWithMessage(this.shardConfigLatest != null, "ShardClient.sendRequestToGroupMembers: null config");
    assertWithMessage(this.shardConfigLatest.groupInfo().containsKey(groupIdManagingShard),
                      "ShardClient.sendRequestToGroupMembers: group managing shard must be in latest config");

    Set<Address> serversInGroup = this.shardConfigLatest.groupInfo().get(groupIdManagingShard).getLeft();
    assertWithMessage(!serversInGroup.isEmpty(), "ShardClient.onClientTimer: should at least be one server in group managing shard");

    broadcast(request, serversInGroup);
  }

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
    assertWithMessage(this.shardConfigLatest != null, "ShardClient.getGroupIdForShard: calling with null config");

    for (Integer groupId : this.shardConfigLatest.groupInfo().keySet()) {
      Pair<Set<Address>, Set<Integer>> groupMetadata = this.shardConfigLatest.groupInfo().get(groupId);

      if (groupMetadata.getRight().contains(shardNum)) { return groupId; }
    }

    // should never get to this point (the server set partitions the shards)
    assertWithMessage(false, "ShardClient.getGroupIdForShard: some group must manage shard");
    return -1;
  }

  private void assertWithMessage(boolean b, String m) {
    if (!b) {
      System.out.println(m);
      System.exit(1);
    }
  }
}
