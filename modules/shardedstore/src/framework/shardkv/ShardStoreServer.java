package framework.shardkv;

import framework.atmostonce.AMOApplication;
import framework.atmostonce.AMOCommand;
import framework.atmostonce.AMOResult;
import framework.Address;
import framework.Application;
import framework.Command;
import framework.kvstore.KVStore;
import framework.kvstore.KVStore.SingleKeyCommand;
import framework.paxos.PaxosDecision;
import framework.paxos.PaxosReply;
import framework.paxos.PaxosRequest;
import framework.paxos.PaxosServer;
import framework.shardmaster.ShardMaster;
import framework.shardmaster.ShardMaster.Query;
import framework.shardmaster.ShardMaster.ShardConfig;
import java.util.HashMap;
import java.util.Set;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NonNull;
import lombok.ToString;

@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public class ShardStoreServer extends ShardStoreNode {
  private final Address[] group;
  private final int groupId;

  private static final int SEQNUM_DONTCARE = -2;

  private static final String PAXOS_ADDRESS_ID = "paxos";
  private Address paxosAddress;

  private final HashMap<Integer, AMOApplication<Application>> amoApplicationSharded;
  private ShardConfig shardConfigLatest;

  // for debugging
  private int paxosLogSlotHighestSeen;

  @Data
  public static final class NewConfig implements Command { private final ShardConfig shardConfig; }

  /* -----------------------------------------------------------------------------------------------
   *  Construction and Initialization
   * ---------------------------------------------------------------------------------------------*/
  ShardStoreServer(
      Address address, Address[] shardMasters, int numShards, Address[] group, int groupId) {
    super(address, shardMasters, numShards);
    this.group = group;
    this.groupId = groupId;

    this.amoApplicationSharded = new HashMap<>();
    this.shardConfigLatest = null;
    this.paxosLogSlotHighestSeen = PaxosServer.LOG_START - 1;
  }

  @Override
  public void init() {
    //
    // setup paxos START
    //
    this.paxosAddress = Address.subAddress(address(), PAXOS_ADDRESS_ID);
    Address[] paxosAddresses = new Address[group.length];
    for (int i = 0; i < paxosAddresses.length; i++) {
      paxosAddresses[i] = Address.subAddress(group[i], PAXOS_ADDRESS_ID);
    }
    PaxosServer paxosServer = new PaxosServer(paxosAddress, paxosAddresses, address());
    addSubNode(paxosServer);
    paxosServer.init();
    //
    // setup paxos END
    //

    sendQueryShardMasters();
    set(new QueryTimer(), QueryTimer.QUERY_RETRY_MILLIS);
  }

  /* -----------------------------------------------------------------------------------------------
   *  Message Handlers
   * ---------------------------------------------------------------------------------------------*/
  private void handleShardStoreRequest(ShardStoreRequest m, Address sender) {
    assertWithThrow(false, "S3.handleShardStoreRequest("+this.address()+"): unimplemented ("+this.shardConfigLatest+"), ("+this.amoApplicationSharded+")");
  }

  private void handlePaxosReply(PaxosReply m, Address sender) {
    AMOResult amoResult = m.result();
    ShardConfig shardConfigNew = (ShardConfig) amoResult.result();

    assertWithThrow(shardConfigNew.configNum() <= getConfigNumForQuery(),
                    "S3.handlePaxosReply: receiving query result with config num too large (" + amoResult.sequenceNum() + ")");

    // config returned from query matches the new expected config number
    if (shardConfigNew.configNum() == getConfigNumForQuery()) {
      // bunch of assertions
      assertWithThrow(shardConfigNew.configNum() == amoResult.sequenceNum(),
                      "S3.handlePaxosReply: config num=" + shardConfigNew.configNum() + " should be same as seq num=" + amoResult.sequenceNum());

      if (this.shardConfigLatest == null) {
        assertWithThrow(shardConfigNew.configNum() == ShardMaster.INITIAL_CONFIG_NUM,
                        "S3.handlePaxosReply: empty config but first query returns non-initial config num");
      } else {
        assertWithThrow(this.shardConfigLatest.configNum() + 1 == shardConfigNew.configNum(),
                        "S3.handlePaxosReply: config in new shard must be one higher than current config (due to first if conditional)");
      }

      process(wrapInDummyAMO(new NewConfig(shardConfigNew)), false);
    }
  }

  private void handlePaxosDecision(PaxosDecision decision, Address sender) {
    assertWithThrow(decision.slotNum() == (this.paxosLogSlotHighestSeen + 1), "S3.handlePaxosDecision: decisions not sent monotonically");
    this.paxosLogSlotHighestSeen++;
    process(decision.amoCommand(), true);
  }

  /* -----------------------------------------------------------------------------------------------
   *  PROCESS helpers
   * ---------------------------------------------------------------------------------------------*/

  private void process(@NonNull AMOCommand amoCommand, boolean isReplicated) {
    if (amoCommand.command() instanceof NewConfig) {
      processNewConfig(amoCommand, isReplicated);
    } else {
      assertWithThrow(false, "S3.process: have not handled non-NewConfig case yet");
    }
  }

  private void processNewConfig(AMOCommand amoCommand, boolean isReplicated) {
    assertWithThrow(amoCommand.command() instanceof NewConfig, "S3.processNewConfig: command must be NewConfig");
    ShardConfig shardConfigNew = ((NewConfig)amoCommand.command()).shardConfig();

    if (!isReplicated) {
      // TODO: add assertions here
      handleMessage(new PaxosRequest(amoCommand), this.paxosAddress);
      return;
    }

    assertWithThrow(shardConfigNew.configNum() == ShardMaster.INITIAL_CONFIG_NUM, "S3.processNewConfig: haven't handled other config numbers yet");

    if (shardConfigNew.groupInfo().containsKey(this.groupId)) {
      assertWithThrow(this.amoApplicationSharded != null && this.amoApplicationSharded.isEmpty(),
          "S3.processNewConfig: map not init to empty");

      // assign this group all the shards
      Set<Integer> shardsInNewConfig = shardConfigNew.groupInfo().get(this.groupId).getRight();
      for (Integer shardNum : shardsInNewConfig) {
        this.amoApplicationSharded.put(shardNum, new AMOApplication<>(new KVStore(), new HashMap<>()));
      }
    }

    this.shardConfigLatest = shardConfigNew;
  }

  /* -----------------------------------------------------------------------------------------------
   *  Timer Handlers
   * ---------------------------------------------------------------------------------------------*/
  private synchronized void onQueryTimer(QueryTimer t) {
    sendQueryShardMasters();
    set(t, QueryTimer.QUERY_RETRY_MILLIS);
  }

  /* -----------------------------------------------------------------------------------------------
   *  Utils
   * ---------------------------------------------------------------------------------------------*/

  private AMOCommand wrapInDummyAMO(Command command) {
    assertWithThrow(!(command instanceof AMOCommand), "S3.wrapInDummyAMO: wrapping command already an AMO");
    return new AMOCommand(command, null, SEQNUM_DONTCARE);
  }

  private void sendQueryShardMasters() {
    Query query = new Query(getConfigNumForQuery());
    // can use the configuration number as the sequence number (since
    // each configuration number has exactly one configuration)
    broadcast(
        new PaxosRequest(new AMOCommand(query, this.address(), getConfigNumForQuery())),
        this.shardMasters()
    );
  }

  // Get the next highest configuration after the latest configuration accepted by
  // this server. Returns INITIAL_CONFIG If this server hasn't yet accepted any configuration
  private int getConfigNumForQuery() {
    if (this.shardConfigLatest == null) {
      assertWithThrow(this.amoApplicationSharded.isEmpty(), "S3.getConfigNumForQuery: app non-empty with empty config");
      return ShardMaster.INITIAL_CONFIG_NUM;
    }
    return this.shardConfigLatest.configNum() + 1;
  }

  private void assertWithThrow(boolean b, String m) {
    if (!b) {
      System.out.println(m);
      System.exit(100);
    }
  }
}
