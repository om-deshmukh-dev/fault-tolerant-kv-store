package framework.shardkv;

import framework.atmostonce.AMOCommand;
import framework.atmostonce.AMOResult;
import framework.Address;
import framework.Client;
import framework.Command;
import framework.Result;
import framework.kvstore.KVStore.SingleKeyCommand;
import framework.kvstore.TransactionalKVStore.Transaction;
import framework.paxos.PaxosReply;
import framework.paxos.PaxosRequest;
import framework.shardmaster.ShardMaster;
import framework.shardmaster.ShardMaster.Error;
import framework.shardmaster.ShardMaster.Query;
import framework.shardmaster.ShardMaster.ShardConfig;
import java.util.HashSet;
import java.util.Set;
import lombok.EqualsAndHashCode;
import lombok.NonNull;
import lombok.ToString;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public class ShardStoreClient extends ShardStoreNode implements Client {
  private ShardConfig shardConfigLatest;
  private int sequenceNumCommands; // for uniquely identifying commands
  private Result result; // for getResult()

  /* -----------------------------------------------------------------------------------------------
   *  Construction and Initialization
   * ---------------------------------------------------------------------------------------------*/
  public ShardStoreClient(Address address, Address[] shardMasters, int numShards) {
    super(address, shardMasters, numShards);
    this.shardConfigLatest = null;
    this.sequenceNumCommands = 0;
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

    if (this.shardConfigLatest != null) {
      sendRequestToGroupMembers(request, computeGroupManagingCommand(command));
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

  // for SingleKeyCommands and Transactions
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
    // ignore errors from ShardMaster
    if (amoResult.result() instanceof Error) {
      return;
    }
    ShardConfig shardConfigNew = (ShardConfig) amoResult.result();

    if (this.shardConfigLatest == null || this.shardConfigLatest.configNum() < shardConfigNew.configNum()) {
      this.shardConfigLatest = shardConfigNew;
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
      int groupIdManagingShard = computeGroupManagingCommand(t.request().command().command());

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

  // Compute the group to send the command to. The group selected depends on
  // the current configuration and the type of command
  //   SingleKeyCommand: Is just the group managing the shard that the command touches
  //   MultiKeyCommand: Among all the groups managing the shards the command touches,
  //                    find the group with the largest identifier
  //
  // This function assumes that the shard configuration is non-null
  private int computeGroupManagingCommand(Command command) {
    assertWithMessage(this.shardConfigLatest != null,
                      "ShardStoreClient.computeGroupManagingCommand: no group manages cmd");

    if (command instanceof SingleKeyCommand singleKeyCommand) {
      return getGroupIdForShard(this.shardConfigLatest, keyToShard(singleKeyCommand.key()));
    }
    else if (command instanceof Transaction transaction) {
      HashSet<Integer> groupsInTransaction = new HashSet<>();

      transaction.keySet().forEach(key -> {
        groupsInTransaction.add(getGroupIdForShard(this.shardConfigLatest, keyToShard(key)));
      });
      return groupsInTransaction.stream().max(Integer::compareTo).get();
    }
    else {
      assertWithMessage(false, "ShardStoreClient.computeGroupManagingCommand: bad cmd");
      return -1;
    }
  }

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
        new PaxosRequest(new AMOCommand(query, this.address(), ShardStoreServer.SEQNUM_DONTCARE)),
        this.shardMasters()
    );
  }

  private void assertWithMessage(boolean b, String m) {
    if (!b) {
      System.out.println(m);
      System.exit(1);
    }
  }
}
