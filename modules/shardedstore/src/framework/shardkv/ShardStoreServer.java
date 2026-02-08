package framework.shardkv;

import com.google.common.collect.Sets;
import framework.atmostonce.AMOApplication;
import framework.atmostonce.AMOApplication.AMOExecution;
import framework.atmostonce.AMOCommand;
import framework.atmostonce.AMOResult;
import framework.Address;
import framework.Application;
import framework.Command;
import framework.Message;
import framework.Result;
import framework.kvstore.KVStore;
import framework.kvstore.KVStore.Get;
import framework.kvstore.KVStore.GetResult;
import framework.kvstore.KVStore.KVStoreResult;
import framework.kvstore.KVStore.Put;
import framework.kvstore.KVStore.SingleKeyCommand;
import framework.kvstore.TransactionalKVStore;
import framework.kvstore.TransactionalKVStore.MultiGet;
import framework.kvstore.TransactionalKVStore.MultiGetResult;
import framework.kvstore.TransactionalKVStore.MultiPut;
import framework.kvstore.TransactionalKVStore.MultiPutOk;
import framework.kvstore.TransactionalKVStore.Swap;
import framework.kvstore.TransactionalKVStore.SwapOk;
import framework.kvstore.TransactionalKVStore.Transaction;
import framework.paxos.PaxosDecision;
import framework.paxos.PaxosReply;
import framework.paxos.PaxosRequest;
import framework.paxos.PaxosServer;
import framework.shardmaster.ShardMaster;
import framework.shardmaster.ShardMaster.Query;
import framework.shardmaster.ShardMaster.ShardConfig;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NonNull;
import lombok.ToString;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public class ShardStoreServer extends ShardStoreNode {
  private final Address[] group;
  private final int groupId;
  private final int numTotalShards;

  public static final int SEQNUM_DONTCARE = -2;

  private static final String PAXOS_ADDRESS_ID = "paxos";
  private Address paxosAddress;

  private final HashMap<Integer, AMOApplication<Application>> amoApplicationSharded;
  private ShardConfig shardConfigLatest;

  // Data structures for 2PC (TWO PHASE COMMIT)
  private final HashMap<Address, AMOExecution> transactionsAlreadyExecuted;
  private final HashMap<TransactionAttempt, TxnCoordState> transactionsOngoingAsCoord;
  private final HashSet<TransactionAttempt> transactionsOngoingAsPart;
  private boolean newConfigSeenWhileTxnOngoing;
  private AMOCommand pendingNewConfigCommand;

  private final Map<Integer, Set<Integer>> reconfigMovesNeeded;
  private final Map<Integer, Set<Integer>> reconfigAcksNeeded;
  private final Queue<AMOCommand> commandsRejectedDuringReconfig;

  @Data
  public static final class NewConfig implements Command { private final ShardConfig shardConfig; }

  @Data
  public static final class ShardMove implements Command {
    private final int groupIdSender; // the group that sent the shards (the group that gets this message is gaining shards)
    private final int configNum;
    private final Map<Integer, AMOApplication<Application>> amoAppShards;
    private final Map<Address, AMOExecution> transactionsAlreadyExecutedSender;
  }

  @Data
  public static final class ShardMoveAck implements Command {
    private final int groupIdReceiver; // the group that received the shards (the group that gets this message is the original sender of the shards)
    private final int configNum;
    private final Map<Integer, AMOApplication<Application>> amoAppShards;
  }

  @Data
  public static final class TPCPrepare implements Command {
    private final int groupIdCoord;
    private final int configNum;
    private final TransactionAttempt txnAttempt;

    public TPCPrepare(int groupIdCoord, int configNum, TransactionAttempt txnAttempt) {
      if (!(txnAttempt.amoTransaction().command() instanceof Transaction)) {
        System.out.println("S3.TPCPrepare: Bad amo command " + txnAttempt.amoTransaction().command());
        System.exit(255);
      }
      this.groupIdCoord = groupIdCoord;
      this.configNum = configNum;
      this.txnAttempt = txnAttempt;
    }
  }

  @Data
  public static final class TPCPrepareOk implements Command {
    private final int groupIdPart;
    private final int configNum;
    private final TransactionAttempt txnAttempt;
    private final MultiGetResult valuesOfTxnKeys;

    public TPCPrepareOk(int groupIdPart, int configNum, TransactionAttempt txnAttempt, MultiGetResult valuesOfTxnKeys) {
      if (!(txnAttempt.amoTransaction().command() instanceof Transaction)) {
        System.out.println("S3.TPCPrepareOk: Bad amo command " + txnAttempt.amoTransaction().command());
        System.exit(255);
      }
      this.groupIdPart = groupIdPart;
      this.configNum = configNum;
      this.txnAttempt = txnAttempt;
      this.valuesOfTxnKeys = valuesOfTxnKeys;
    }
  }

  @Data
  public static final class TPCCommit implements Command {
    private final int groupIdCoord;
    private final int configNum;
    private final TransactionAttempt txnAttempt;
    private final MultiGetResult valuesOfTxnKeys;

    public TPCCommit(int groupIdCoord, int configNum, TransactionAttempt txnAttempt, MultiGetResult valuesOfTxnKeys) {
      if (!(txnAttempt.amoTransaction().command() instanceof Transaction)) {
        System.out.println("S3.TPCCommit: Bad amo command " + txnAttempt.amoTransaction().command());
        System.exit(255);
      }
      this.groupIdCoord = groupIdCoord;
      this.configNum = configNum;
      this.txnAttempt = txnAttempt;
      this.valuesOfTxnKeys = valuesOfTxnKeys;
    }
  }

  @Data
  public static final class TPCCommitOk implements Command {
    private final int groupIdPart;
    private final int configNum;
    private final TransactionAttempt txnAttempt;

    public TPCCommitOk(int groupIdPart, int configNum, TransactionAttempt txnAttempt) {
      if (!(txnAttempt.amoTransaction().command() instanceof Transaction)) {
        System.out.println("S3.TPCCommitOk: Bad amo command " + txnAttempt.amoTransaction().command());
        System.exit(255);
      }
      this.groupIdPart = groupIdPart;
      this.configNum = configNum;
      this.txnAttempt = txnAttempt;
    }
  }

  @Data
  public static final class TPCPrepareAbort implements Command {
    private final int groupIdPart;
    private final int configNum;
    private final TransactionAttempt txnAttempt;

    public TPCPrepareAbort(int groupIdPart, int configNum, TransactionAttempt txnAttempt) {
      if (!(txnAttempt.amoTransaction().command() instanceof Transaction)) {
        System.out.println("S3.TPCPrepareAbort: Bad amo command " + txnAttempt.amoTransaction().command());
        System.exit(255);
      }
      this.groupIdPart = groupIdPart;
      this.configNum = configNum;
      this.txnAttempt = txnAttempt;
    }
  }

  @Data
  public static final class TPCAbort implements Command {
    private final int groupIdCoord;
    private final int configNum;
    private final TransactionAttempt txnAttempt;

    public TPCAbort(int groupIdCoord, int configNum, TransactionAttempt txnAttempt) {
      if (!(txnAttempt.amoTransaction().command() instanceof Transaction)) {
        System.out.println("S3.TPCAbort: Bad amo command " + txnAttempt.amoTransaction().command());
        System.exit(255);
      }
      this.groupIdCoord = groupIdCoord;
      this.configNum = configNum;
      this.txnAttempt = txnAttempt;
    }
  }

  @Data
  public static final class TPCAbortOk implements Command {
    private final int groupIdPart;
    private final int configNum;
    private final TransactionAttempt txnAttempt;

    public TPCAbortOk(int groupIdPart, int configNum, TransactionAttempt txnAttempt) {
      if (!(txnAttempt.amoTransaction().command() instanceof Transaction)) {
        System.out.println("S3.TPCAbortOk: Bad amo command " + txnAttempt.amoTransaction().command());
        System.exit(255);
      }
      this.groupIdPart = groupIdPart;
      this.configNum = configNum;
      this.txnAttempt = txnAttempt;
    }
  }

  // Wrapper for transaction with retry number
  @Data
  public static final class TransactionAttempt {
    private final AMOCommand amoTransaction;
    private final int retryNum;

    public TransactionAttempt(AMOCommand amoTransaction, int retryNum) {
      if (!(amoTransaction.command() instanceof Transaction)) {
        System.out.println("S3.TransactionAttempt: Bad amo command " + amoTransaction.command());
        System.exit(255);
      }
      this.amoTransaction = amoTransaction;
      this.retryNum = retryNum;
    }

    // equality only depends on amoTransaction, not retryNum
    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      TransactionAttempt that = (TransactionAttempt) o;
      return amoTransaction.equals(that.amoTransaction);
    }

    @Override
    public int hashCode() {
      return amoTransaction.hashCode();
    }
  }

  // State held at coordinator for an ongoing transaction
  @EqualsAndHashCode
  @ToString
  public static final class TxnCoordState {
    private final HashSet<TPCPrepareOk> prepareOksReceived;
    private final HashSet<TPCCommitOk> commitOksReceived;
    private final HashSet<TPCAbortOk> abortOksReceived;
    private boolean isAborted;

    public TxnCoordState() {
      this.prepareOksReceived = new HashSet<>();
      this.commitOksReceived = new HashSet<>();
      this.abortOksReceived = new HashSet<>();
      this.isAborted = false;
    }

    public HashSet<TPCPrepareOk> prepareOksReceived() { return prepareOksReceived; }
    public HashSet<TPCCommitOk> commitOksReceived() { return commitOksReceived; }
    public HashSet<TPCAbortOk> abortOksReceived() { return abortOksReceived; }
    public boolean isAborted() { return isAborted; }
    public void setAborted(boolean aborted) { this.isAborted = aborted; }
  }

  /* -----------------------------------------------------------------------------------------------
   *  Construction and Initialization
   * ---------------------------------------------------------------------------------------------*/
  ShardStoreServer(
      Address address, Address[] shardMasters, int numShards, Address[] group, int groupId) {
    super(address, shardMasters, numShards);
    this.group = group;
    this.groupId = groupId;
    this.numTotalShards = numShards;

    this.amoApplicationSharded = new HashMap<>();
    this.shardConfigLatest = null;

    this.transactionsAlreadyExecuted = new HashMap<>();
    this.transactionsOngoingAsCoord = new HashMap<>();
    this.transactionsOngoingAsPart = new HashSet<>();
    this.newConfigSeenWhileTxnOngoing = false;
    this.pendingNewConfigCommand = null;

    this.reconfigMovesNeeded = new HashMap<>();
    this.reconfigAcksNeeded = new HashMap<>();
    this.commandsRejectedDuringReconfig = new LinkedList<>();
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
    set(new DriveOngoingTransactionsTimer(), DriveOngoingTransactionsTimer.DRIVE_TXN_MILLIS);
  }

  /* -----------------------------------------------------------------------------------------------
   *  Message Handlers
   * ---------------------------------------------------------------------------------------------*/
  private void handleShardStoreRequest(ShardStoreRequest m, Address sender) {
    process(m.command(), false);
  }

  private void handleShardStoreShardMove(ShardStoreShardMove m, Address sender) {
    if (this.shardConfigLatest == null) {
      return; // wait for ShardMaster first query result first before changing config
    } else if (m.shardMove().configNum() > this.shardConfigLatest.configNum()) {
      return; // do not process move with higher config num
    }

    // at this point, the config num in the message must be at most
    // as large as the config num in the latest configuration

    // if already processed the ShardMove, send back a redundant ack (with the sender's config num)
    if (m.shardMove().configNum() < this.shardConfigLatest.configNum() || !isReconfigOngoing()) {
      broadcast(new ShardStoreShardMoveAck(
            new ShardMoveAck(this.groupId, m.shardMove().configNum(), m.shardMove().amoAppShards())
          ),
          getServersForGroupId(this.shardConfigLatest, m.shardMove().groupIdSender())
      );
      // TODO: the sending group may have left in latest configuration, but is far behind
      //       (so this server does not know who to send to)
      return;
    }

    if (!this.reconfigMovesNeeded.containsKey(m.shardMove().groupIdSender())) {
      return; // already received the move (or reconfig is not ongoing)
    }

    // reconfig ongoing, move for current config, sender is expected
    process(wrapInDummyAMO(m.shardMove()), false);
  }

  private void handleShardStoreShardMoveAck(ShardStoreShardMoveAck m, Address sender) {
    if (this.shardConfigLatest == null) {
      return; // other members of group are far ahead
    }
    if (m.shardMoveAck().configNum() != this.shardConfigLatest.configNum() || !isReconfigOngoing()) {
      return; // ack is for different config or reconfig not ongoing
    }
    if (!this.reconfigAcksNeeded.containsKey(m.shardMoveAck().groupIdReceiver())) {
      return; // already received ack
    }

    // at this point, reconfig is ongoing, config num in ack matches, and this group is still expecting ack from the receiver
    process(wrapInDummyAMO(m.shardMoveAck()), false);
  }

  // from a ShardMaster query
  private void handlePaxosReply(PaxosReply m, Address sender) {
    if (isReconfigOngoing()) {
      return; // can't do much with a new configuration while an older one is being processed
    }

    AMOResult amoResult = m.result();
    
    // ignore errors from ShardMaster, retry on next query
    if (amoResult.result() instanceof ShardMaster.Error) {
      return;
    }
    
    ShardConfig shardConfigNew = (ShardConfig) amoResult.result();

    // new config num should be at most as large as the new config this server is querying for
    assertWithThrow(shardConfigNew.configNum() <= getConfigNumForQuery(),
                    "S3.handlePaxosReply: receiving query result with config num too large");

    // config returned from query matches the new expected config number,
    // send reconfiguration request to paxos subnode
    if (shardConfigNew.configNum() == getConfigNumForQuery()) {
      // bunch of assertions
      if (this.shardConfigLatest == null) {
        assertWithThrow(shardConfigNew.configNum() == ShardMaster.INITIAL_CONFIG_NUM,
                        "S3.handlePaxosReply: empty config but first query returns non-initial config num");
      }
      else {
        assertWithThrow(this.shardConfigLatest.configNum() + 1 == shardConfigNew.configNum(),
                        "S3.handlePaxosReply: config in new shard must be one higher than current config (due to first if conditional)");
      }

      process(wrapInDummyAMO(new NewConfig(shardConfigNew)), false);
    }
  }

  private void handlePaxosDecision(PaxosDecision decision, Address sender) {
    process(decision.amoCommand(), true);
  }

  // for 2PC
  private void handleShardStoreTPCPrepare(ShardStoreTPCPrepare m, Address sender) {
    process(wrapInDummyAMO(m.tpcPrepare()), false);
  }

  private void handleShardStoreTPCPrepareOk(ShardStoreTPCPrepareOk m, Address sender) {
    process(wrapInDummyAMO(m.tpcPrepareOk()), false);
  }

  private void handleShardStoreTPCCommit(ShardStoreTPCCommit m, Address sender) {
    process(wrapInDummyAMO(m.tpcCommit()), false);
  }

  private void handleShardStoreTPCCommitOk(ShardStoreTPCCommitOk m, Address sender) {
    process(wrapInDummyAMO(m.tpcCommitOk()), false);
  }

  private void handleShardStoreTPCPrepareAbort(ShardStoreTPCPrepareAbort m, Address sender) {
    process(wrapInDummyAMO(m.tpcPrepareAbort()), false);
  }

  private void handleShardStoreTPCAbort(ShardStoreTPCAbort m, Address sender) {
    process(wrapInDummyAMO(m.tpcAbort()), false);
  }

  private void handleShardStoreTPCAbortOk(ShardStoreTPCAbortOk m, Address sender) {
    process(wrapInDummyAMO(m.tpcAbortOk()), false);
  }

  /* -----------------------------------------------------------------------------------------------
   *  PROCESS helpers
   * ---------------------------------------------------------------------------------------------*/

  private void process(@NonNull AMOCommand amoCommand, boolean isReplicated) {
    if (amoCommand.command() instanceof NewConfig) {
      processNewConfig(amoCommand, isReplicated);
    } else if (amoCommand.command() instanceof SingleKeyCommand) {
      processSingleKeyCommand(amoCommand, isReplicated);
    } else if (amoCommand.command() instanceof Transaction) {
      processTransaction(amoCommand, isReplicated);
    } else if (amoCommand.command() instanceof TPCPrepare) {
      processTPCPrepare(amoCommand, isReplicated);
    } else if (amoCommand.command() instanceof TPCPrepareOk) {
      processTPCPrepareOk(amoCommand, isReplicated);
    } else if (amoCommand.command() instanceof TPCCommit) {
      processTPCCommit(amoCommand, isReplicated);
    } else if (amoCommand.command() instanceof  TPCCommitOk) {
      processTPCCommitOk(amoCommand, isReplicated);
    } else if (amoCommand.command() instanceof TPCPrepareAbort) {
      processTPCPrepareAbort(amoCommand, isReplicated);
    } else if (amoCommand.command() instanceof TPCAbort) {
      processTPCAbort(amoCommand, isReplicated);
    } else if (amoCommand.command() instanceof TPCAbortOk) {
      processTPCAbortOk(amoCommand, isReplicated);
    } else if (amoCommand.command() instanceof ShardMove) {
      processShardMoveCommand(amoCommand, isReplicated);
    } else if (amoCommand.command() instanceof ShardMoveAck) {
      processShardMoveAckCommand(amoCommand, isReplicated);
    } else {
      assertWithThrow(false, "S3.process: bad type");
    }
  }

  private void processShardMoveAckCommand(@NonNull AMOCommand amoCommand, boolean isReplicated) {
    if (!isReplicated) {
      handleMessage(new PaxosRequest(amoCommand), this.paxosAddress);
      return;
    }

    ShardMoveAck shardMoveAckToUs = (ShardMoveAck) amoCommand.command();

    if (!isReconfigOngoing()) {
      return; // duplicated shard ack
    } else if (shardMoveAckToUs.configNum() != this.shardConfigLatest.configNum()) {
      return;
    }

    if (this.reconfigAcksNeeded.containsKey(shardMoveAckToUs.groupIdReceiver())) {
      assertWithThrow(this.reconfigAcksNeeded.get(shardMoveAckToUs.groupIdReceiver()).equals(shardMoveAckToUs.amoAppShards.keySet()),
                      "S3.processShardMoveAckCommand: receiving unexpected shards from ack");

      // remove all ack'd shards from this server's amoAppSharded,
      // then remove receiver from ReconfigAcksNeeded
      shardMoveAckToUs.amoAppShards.keySet().forEach((shard) -> {
        assertWithThrow(this.amoApplicationSharded.containsKey(shard), "S3.processShardMoveAckCommand: somehow lost shard already");
        this.amoApplicationSharded.remove(shard);
      });
      this.reconfigAcksNeeded.remove(shardMoveAckToUs.groupIdReceiver());

      if (!isReconfigOngoing()) {
        assertWithThrow(this.reconfigMovesNeeded.isEmpty(), "S3.processShardMoveAckCommand: acks done but moves still needed");
        processRejectedCommands();
      }
    }
  }

  private void processShardMoveCommand(@NonNull AMOCommand amoCommand, boolean isReplicated) {
    if (!isReplicated) {
      handleMessage(new PaxosRequest(amoCommand), this.paxosAddress);
      return;
    }

    ShardMove shardMoveToUs = (ShardMove) amoCommand.command();

    if (!isReconfigOngoing()) {
      return; // duplicated shard move
    } else if (shardMoveToUs.configNum() != this.shardConfigLatest.configNum()) {
      return;
    }

    if (this.reconfigMovesNeeded.containsKey(shardMoveToUs.groupIdSender())) {
      assertWithThrow(this.reconfigMovesNeeded.get(shardMoveToUs.groupIdSender()).equals(shardMoveToUs.amoAppShards.keySet()),
          "S3.processShardMoveCommand: receiving unexpected shards");

      // merge the sharded application state into this server's amoApp
      shardMoveToUs.amoAppShards.forEach((shard, amoApp) -> {
        assertWithThrow(!this.amoApplicationSharded.containsKey(shard), "S3.processShardMoveCommand: already have shard somehow");
        this.amoApplicationSharded.put(shard, amoApp);
      });

      // merge the transaction state from the ShardMove message
      shardMoveToUs.transactionsAlreadyExecutedSender().forEach((client, execution) -> {
        if (!this.transactionsAlreadyExecuted.containsKey(client)
          || this.transactionsAlreadyExecuted.get(client).amoCommand().sequenceNum() < execution.amoCommand().sequenceNum()
        ) {
            this.transactionsAlreadyExecuted.put(client, execution);
        }
      });

      // remove group from ReconfigMovesNeeded, and send Ack with this group's group ID embedded
      this.reconfigMovesNeeded.remove(shardMoveToUs.groupIdSender());
      broadcast(
          new ShardStoreShardMoveAck(
              new ShardMoveAck(this.groupId, shardMoveToUs.configNum(), shardMoveToUs.amoAppShards())
          ),
          getServersForGroupId(this.shardConfigLatest, shardMoveToUs.groupIdSender())
      );

      if (!isReconfigOngoing()) {
        assertWithThrow(this.reconfigAcksNeeded.isEmpty(), "S3.processShardMoveCommand: moves done but acks still needed");
        processRejectedCommands();
      }
    }

  }

  private void processSingleKeyCommand(@NonNull AMOCommand amoCommand, boolean isReplicated) {
    assertWithThrow(amoCommand.command() instanceof SingleKeyCommand, "S3.processSingleKeyCommand: called with wrong command type");

    if (isReconfigOngoing()) {
      if (isReplicated) { this.commandsRejectedDuringReconfig.add(amoCommand); }
      return;
    }

    // TODO: make sure the lock on this key is not acquired already
    assertWithThrow(!isSomeTxnOngoing(), "S3.processSingleKeyCommand: should check that the key in the command is not locked");

    // check if this group is managing the shard
    SingleKeyCommand singleKeyCommand = (SingleKeyCommand) amoCommand.command();
    if (!isManagingCommand(singleKeyCommand)) {
      return;
    }

    // check if this group has already executed the command
    AMOApplication<Application> amoAppFromShard = this.amoApplicationSharded.get(keyToShard(singleKeyCommand.key()));
    if (amoAppFromShard.alreadyExecuted(amoCommand)) {
      AMOResult amoResult = amoAppFromShard.execute(amoCommand);
      send(new ShardStoreReply(amoResult), amoCommand.address());
      return;
    }

    // group manages shard, either propose to paxos subnode or execute command (depending on if replicated)
    if (!isReplicated) {
      handleMessage(new PaxosRequest(amoCommand), paxosAddress);
    } else {
      // execute the command and send result back to client
      int shardContainingKey = keyToShard(singleKeyCommand.key());
      AMOResult amoResult = this.amoApplicationSharded.get(shardContainingKey).execute(amoCommand);
      send(new ShardStoreReply(amoResult), amoCommand.address());
    }
  }

  private void processNewConfig(AMOCommand amoCommand, boolean isReplicated) {
    ShardConfig shardConfigNew = ((NewConfig)amoCommand.command()).shardConfig();

    // TODO: may be able to assert that config in decision is at most one higher than current config
    // assertWithThrow(!isReconfigOngoing(), "S3.processNewConfig: handle reconfig case");

    if (!isReplicated) {
      // TODO: add assertions here
      handleMessage(new PaxosRequest(amoCommand), this.paxosAddress);
      return;
    }

    // assert config in decision is either initial or at most one higher than current
    assertWithThrow(
        (this.shardConfigLatest == null && shardConfigNew.configNum() == ShardMaster.INITIAL_CONFIG_NUM) ||
            (this.shardConfigLatest != null && shardConfigNew.configNum() <= this.shardConfigLatest.configNum() + 1),
        "S3.processNewConfig: config must be INITIAL or at most one higher"
    );

    // if reconfig ongoing, new config must be at most current config
    if (isReconfigOngoing()) {
      assertWithThrow(
          this.shardConfigLatest != null && shardConfigNew.configNum() <= this.shardConfigLatest.configNum(),
          "S3.processNewConfig: reconfig ongoing but decision for higher config"
      );
      return;
    }

    if (this.shardConfigLatest != null && shardConfigNew.configNum() <= this.shardConfigLatest.configNum()) {
      // duplicated decision (already moved on to new config)
      return;
    }

    if (this.shardConfigLatest == null) {
      // initial configuration case
      assertWithThrow(shardConfigNew.configNum() == ShardMaster.INITIAL_CONFIG_NUM,
                      "S3.processNewConfig: config empty but not getting INITIAL_CONFIG");

      // this group manages all the shards, initialize sharded AMO app accordingly
      if (shardConfigNew.groupInfo().containsKey(this.groupId)) {
        assertWithThrow(this.amoApplicationSharded != null && this.amoApplicationSharded.isEmpty(), "S3.processNewConfig: map not init to empty");

        // assign this group all the shards
        Set<Integer> shardsInNewConfig = shardConfigNew.groupInfo().get(this.groupId).getRight();
        for (Integer shardNum : shardsInNewConfig) {
          this.amoApplicationSharded.put(shardNum, new AMOApplication<>(new TransactionalKVStore(), new HashMap<>()));
        }
      }
    } else {
      // assertWithThrow(!isSomeTxnOngoing(), "S3.processNewConfig: handle case where new config comes but txn ongoing");
      // if a transaction is ongoing while we see a new config, set the flag
      if (isSomeTxnOngoing()) {
        this.newConfigSeenWhileTxnOngoing = true;
        this.pendingNewConfigCommand = amoCommand;
        // abort ongoing transactions as coordinator that are NOT yet in commit phase
        for (TransactionAttempt txnAttempt : this.transactionsOngoingAsCoord.keySet()) {
          TxnCoordState coordState = this.transactionsOngoingAsCoord.get(txnAttempt);
          Transaction transaction = (Transaction) txnAttempt.amoTransaction().command();
          int numParticipants = getTransactionParticipants(transaction, this.shardConfigLatest).size();
          // only abort if not all PrepareOks received (not yet in commit phase)
          boolean inCommitPhase = coordState.prepareOksReceived().size() >= numParticipants - 1;
          if (!coordState.isAborted() && !inCommitPhase) {
            coordState.setAborted(true);
            sendAbortToAllParticipants(txnAttempt, transaction);
          }
        }
        return;
      }

      setupReconfigDS(shardConfigNew);
    }

    // take on the new configuration (which must be higher)
    this.shardConfigLatest = shardConfigNew;
    // reset the flag when we move on to a new configuration
    this.newConfigSeenWhileTxnOngoing = false;
    this.pendingNewConfigCommand = null;
  }

  private void processTransaction(@NonNull AMOCommand amoTransaction, boolean isReplicated) {
    assertWithThrow(amoTransaction.command() instanceof Transaction, "S3.processTransaction: called with wrong command type");

    if (isReconfigOngoing()) {
      assertWithThrow(!isSomeTxnOngoing(), "S3.processTransaction: no txn should be ongoing during reconfig");
      return;
    }

    // check if this group is managing the transaction (coordinator)
    Transaction transaction = (Transaction) amoTransaction.command();
    if (!isManagingCommand(transaction)) {
      return;
    }

    // if the transaction has already been executed, then reply back to the client
    if (isTxnAlreadyExecuted(amoTransaction)) {
      send(new ShardStoreReply(getResultOfTransaction(amoTransaction)), amoTransaction.address());
      return;
    }

    // coordinator should not process new transaction decisions while newConfigSeenWhileTxnOngoing
    if (this.newConfigSeenWhileTxnOngoing) {
      return;
    }

    // already acquired locks for transaction (ongoing), can just continue with it
    TransactionAttempt existingAttempt = getOngoingTxnAttemptAsCoord(amoTransaction);
    if (existingAttempt != null) {
      return;
    }

    // drop the transaction if this group cannot acquire the locks for it
    if (!canAcquireLocks(amoTransaction)) {
      return;
    }

    // transaction not handled before, either propose or execute
    if (!isReplicated) {
      handleMessage(new PaxosRequest(amoTransaction), this.paxosAddress);
    } else {

      if (getTransactionParticipants(transaction, this.shardConfigLatest).size() == 1) {
        // this group can execute the entire transaction at once (no 2PC case)
        txnDecomposeAndExecute(amoTransaction, getValuesOfKeysManagedInTxn(amoTransaction));
        send(new ShardStoreReply(getResultOfTransaction(amoTransaction)), amoTransaction.address());
      } else {
        // 2PC case, this group is the coordinator, setup DS, and send prepares to all participants
        TransactionAttempt txnAttempt = new TransactionAttempt(amoTransaction, 0);
        assertWithThrow(!this.transactionsOngoingAsCoord.containsKey(txnAttempt), "S3.processTransaction: starting new txn but it's already ongoing");

        this.transactionsOngoingAsCoord.put(txnAttempt, new TxnCoordState());
        sendAllExceptSelf(
            new ShardStoreTPCPrepare(
                new TPCPrepare(this.groupId, this.shardConfigLatest.configNum(), txnAttempt)
            ),
            getTransactionParticipants(transaction, this.shardConfigLatest),
            true
        );
      }
    }
  }

  private void processTPCPrepare(@NonNull AMOCommand amoCommand, boolean isReplicated) {
    TPCPrepare tpcPrepare = (TPCPrepare) amoCommand.command();
    TransactionAttempt txnAttempt = tpcPrepare.txnAttempt();
    Transaction transaction = (Transaction) txnAttempt.amoTransaction().command();

    if (!isReplicated) {
      handleMessage(new PaxosRequest(amoCommand), this.paxosAddress);
      return;
    }

    // can drop transaction if already executed (duplicated prepare, coordinator already committed)
    if (isTxnAlreadyExecuted(txnAttempt.amoTransaction())) {
      return;
    }

    // assertWithThrow(!isReconfigOngoing(), "S3.processTPCPrepare: reconfig ongoing case (send abort)");
    // send PrepareAbort if reconfig is ongoing
    if (isReconfigOngoing()) {
      sendToGroup(
          new ShardStoreTPCPrepareAbort(
              new TPCPrepareAbort(this.groupId, this.shardConfigLatest.configNum(), txnAttempt)
          ),
          tpcPrepare.groupIdCoord()
      );
      return;
    }

    // assertWithThrow(this.shardConfigLatest != null && tpcPrepare.configNum() == this.shardConfigLatest.configNum(), "S3.processTPCPrepare: config mismatch (send abort)");
    // send PrepareAbort if config num does not match
    if (this.shardConfigLatest == null || tpcPrepare.configNum() != this.shardConfigLatest.configNum()) {
      int configNumToSend = this.shardConfigLatest != null ? this.shardConfigLatest.configNum() : -1;
      sendToGroup(
          new ShardStoreTPCPrepareAbort(
              new TPCPrepareAbort(this.groupId, configNumToSend, txnAttempt)
          ),
          tpcPrepare.groupIdCoord()
      );
      return;
    }

    assertWithThrow(!isManagingCommand(txnAttempt.amoTransaction().command()), "S3.processTPCPrepare: somehow got prepare when managing command in same config as sender (BAD)");

    // if transaction is already ongoing at this participant
    TransactionAttempt existingAttempt = getOngoingTxnAttemptAsPart(txnAttempt.amoTransaction());
    if (existingAttempt != null) {
      // if incoming retry # is higher, update the attempt
      if (txnAttempt.retryNum() > existingAttempt.retryNum()) {
        this.transactionsOngoingAsPart.remove(existingAttempt);
        this.transactionsOngoingAsPart.add(txnAttempt);
      }
      // send back PrepareOk (could have been lost)
      TransactionAttempt attemptToUse = txnAttempt.retryNum() >= existingAttempt.retryNum() ? txnAttempt : existingAttempt;
      ShardStoreTPCPrepareOk tpcPrepareOk = new ShardStoreTPCPrepareOk(
          new TPCPrepareOk(
              this.groupId, this.shardConfigLatest.configNum(),
              attemptToUse,
              getValuesOfKeysManagedInTxn(attemptToUse.amoTransaction())
          )
      );
      sendToGroup(tpcPrepareOk, tpcPrepare.groupIdCoord());
      return;
    }

    // transaction not already ongoing, check if locks can be acquired
    assertWithThrow(!locksAcquiredAsCoordinator(txnAttempt.amoTransaction()), "S3.processTPCPrepare: for transaction T, this group and the sender cannot simultaneously be coordinator (BAD)");

    // assertWithThrow(canAcquireLocks(txnAttempt.amoTransaction()), "S3.processTPCPrepare: locks cannot be acquired (should send abort)");
    if (!canAcquireLocks(txnAttempt.amoTransaction())) {
      // send PrepareAbort if locks cannot be acquired
      sendToGroup(
          new ShardStoreTPCPrepareAbort(
              new TPCPrepareAbort(this.groupId, this.shardConfigLatest.configNum(), txnAttempt)
          ),
          tpcPrepare.groupIdCoord()
      );
      return;
    }

    // acquire locks and send PrepareOk
    this.transactionsOngoingAsPart.add(txnAttempt);
    ShardStoreTPCPrepareOk tpcPrepareOk = new ShardStoreTPCPrepareOk(
        new TPCPrepareOk(
            this.groupId, this.shardConfigLatest.configNum(),
            txnAttempt,
            getValuesOfKeysManagedInTxn(txnAttempt.amoTransaction())
        )
    );

    // reply only to the coordinator
    sendToGroup(tpcPrepareOk, tpcPrepare.groupIdCoord());
  }

  private void processTPCPrepareOk(@NonNull AMOCommand amoCommand, boolean isReplicated) {
    TPCPrepareOk tpcPrepareOk = (TPCPrepareOk) amoCommand.command();
    TransactionAttempt txnAttempt = tpcPrepareOk.txnAttempt();
    Transaction transaction = (Transaction) txnAttempt.amoTransaction().command();

    if (!isReplicated) {
      handleMessage(new PaxosRequest(amoCommand), this.paxosAddress);
      return;
    }

    // replicated:

    if (isTxnAlreadyExecuted(txnAttempt.amoTransaction())) {
      return;
    }

    TransactionAttempt existingAttempt = getOngoingTxnAttemptAsCoord(txnAttempt.amoTransaction());
    if (existingAttempt == null) {
      // transaction no longer ongoing as coordinator
      return;
    }

    // can only accept PrepareOk with matching retry number
    if (txnAttempt.retryNum() != existingAttempt.retryNum()) {
      return;
    }

    TxnCoordState coordState = this.transactionsOngoingAsCoord.get(existingAttempt);
    // assertWithThrow(!coordState.isAborted(), "S3.processTPCPrepareOk: transaction is aborted, should not receive PrepareOk");
    if (coordState.isAborted()) {
      return;
    }

    assertWithThrow(!isReconfigOngoing(), "S3.processTPCPrepareOk: reconfig ongoing (config nums should not match then)");
    assertWithThrow(tpcPrepareOk.configNum() <= this.shardConfigLatest.configNum(), "S3.processTPCPrepareOk: config num in PrepareOk must be at most current config");
    assertWithThrow(isManagingCommand(transaction), "S3.processTPCPrepareOk: got prepare ok but not manager (coordinator) of transaction");

    HashSet<TPCPrepareOk> prepareOks = coordState.prepareOksReceived();
    prepareOks.add(tpcPrepareOk);

    // once every other group has sent back a PrepareOk, this group (coordinator) can COMMIT
    // the transaction by executing it, then sending a COMMIT message to all
    if (prepareOks.size() == getTransactionParticipants(transaction, this.shardConfigLatest).size() - 1) {

      MultiGetResult valuesOfKeysMerged = getValuesOfKeysManagedInTxn(txnAttempt.amoTransaction());
      for (TPCPrepareOk prepareOkReceived : prepareOks) {
        assertWithThrow(Sets.intersection(prepareOkReceived.valuesOfTxnKeys().values().keySet(), valuesOfKeysMerged.values().keySet()).isEmpty(), "S3.processTPCPrepare: read keys must be disjoint among all participants");
        valuesOfKeysMerged.values().putAll(prepareOkReceived.valuesOfTxnKeys().values());
      }

      // if keys don't match (can happen with pending config), abort instead of crash
      if (!transaction.keySet().equals(valuesOfKeysMerged.values().keySet())) {
        coordState.setAborted(true);
        sendAbortToAllParticipants(existingAttempt, transaction);
        return;
      }

      // execute TXN, send COMMIT to all other groups involved in TXN
      txnDecomposeAndExecute(txnAttempt.amoTransaction(), valuesOfKeysMerged);
      sendAllExceptSelf(
          new ShardStoreTPCCommit(
              new TPCCommit(this.groupId, this.shardConfigLatest.configNum(),
                  existingAttempt, valuesOfKeysMerged)
          ),
          getTransactionParticipants(transaction, this.shardConfigLatest),
          true
      );
    }
  }

  private void processTPCCommit(@NonNull AMOCommand amoCommand, boolean isReplicated) {
    TPCCommit tpcCommit = (TPCCommit) amoCommand.command();
    TransactionAttempt txnAttempt = tpcCommit.txnAttempt();
    Transaction transaction = (Transaction) txnAttempt.amoTransaction().command();

    if (!isReplicated) {
      handleMessage(new PaxosRequest(amoCommand), this.paxosAddress);
      return;
    }

    // replicated:

    // message to send back (if the commit is successful here)
    ShardStoreTPCCommitOk commitOkMsg = new ShardStoreTPCCommitOk(
        new TPCCommitOk(this.groupId, this.shardConfigLatest.configNum(), txnAttempt)
    );

    TransactionAttempt existingAttempt = getOngoingTxnAttemptAsPart(txnAttempt.amoTransaction());
    if (existingAttempt == null) {
      // assertWithThrow(isTxnAlreadyExecuted(txnAttempt.amoTransaction()), "S3.processTPCCommit: locks released, but committed transaction not already executed (BAD)");
      if (isTxnAlreadyExecuted(txnAttempt.amoTransaction())) {
        sendToGroup(commitOkMsg, tpcCommit.groupIdCoord());
      }
      return;
    }

    // assertWithThrow(txnAttempt.retryNum() == existingAttempt.retryNum(), "S3.processTPCCommit: retry number mismatch");
    if (txnAttempt.retryNum() != existingAttempt.retryNum()) {
      return;
    }
    assertWithThrow(this.shardConfigLatest.configNum() == tpcCommit.configNum(), "S3.processTPCCommit: config num mismatch");
    assertWithThrow(!isReconfigOngoing(), "S3.processTPCCommit: reconfiguration ongoing (should not be possible at this point)");
    // assertWithThrow(!isTxnAlreadyExecuted(txnAttempt.amoTransaction()), "S3.processTPCCommit: somehow locks acquired, but txn already executed");
    if (isTxnAlreadyExecuted(txnAttempt.amoTransaction())) {
      this.transactionsOngoingAsPart.remove(existingAttempt);
      sendToGroup(commitOkMsg, tpcCommit.groupIdCoord());
      return;
    }

    txnDecomposeAndExecute(txnAttempt.amoTransaction(), tpcCommit.valuesOfTxnKeys());
    this.transactionsOngoingAsPart.remove(existingAttempt);
    sendToGroup(commitOkMsg, tpcCommit.groupIdCoord());

    // if no more ongoing transactions (as participant), re-process pending config
    if (!isSomeTxnOngoing() && this.pendingNewConfigCommand != null) {
      AMOCommand pendingConfig = this.pendingNewConfigCommand;
      this.pendingNewConfigCommand = null;
      this.newConfigSeenWhileTxnOngoing = false;
      processNewConfig(pendingConfig, true);
    }
  }

  private void processTPCCommitOk(@NonNull AMOCommand amoCommand, boolean isReplicated) {
    TPCCommitOk tpcCommitOk = (TPCCommitOk) amoCommand.command();
    TransactionAttempt txnAttempt = tpcCommitOk.txnAttempt();
    Transaction transaction = (Transaction) txnAttempt.amoTransaction().command();
    Address client = txnAttempt.amoTransaction().address();

    if (!isReplicated) {
      handleMessage(new PaxosRequest(amoCommand), this.paxosAddress);
      return;
    }

    // replicated after this point:

    TransactionAttempt existingAttempt = getOngoingTxnAttemptAsCoord(txnAttempt.amoTransaction());
    if (existingAttempt == null) {
      // transaction no longer ongoing as coordinator, must have already completed
      assertWithThrow(isTxnAlreadyExecuted(txnAttempt.amoTransaction()), "S3.processTPCCommitOk: locks released on committed transaction, but not already executed (BAD)");
      send(new ShardStoreReply(getResultOfTransaction(txnAttempt.amoTransaction())), client);
      return;
    }

    TxnCoordState coordState = this.transactionsOngoingAsCoord.get(existingAttempt);
    // assertWithThrow(txnAttempt.retryNum() == existingAttempt.retryNum(), "S3.processTPCCommitOk: retry number mismatch");
    // assertWithThrow(!coordState.isAborted(), "S3.processTPCCommitOk: transaction is aborted, should not receive CommitOk");
    if (txnAttempt.retryNum() != existingAttempt.retryNum() || coordState.isAborted()) {
      return;
    }
    // assertWithThrow(!isReconfigOngoing(), "S3.processTPCCommitOk: reconfiguration ongoing (should not be possible at this point)");
    // assertWithThrow(tpcCommitOk.configNum() <= this.shardConfigLatest.configNum(), "S3.processTPCCommitOk: config num in CommitOk must be at most current config");
    // participant may have moved to a higher config after committing, which is fine

    HashSet<TPCCommitOk> commitOks = coordState.commitOksReceived();
    commitOks.add(tpcCommitOk);

    if (commitOks.size() == getTransactionParticipants(transaction, this.shardConfigLatest).size() - 1) {
      this.transactionsOngoingAsCoord.remove(existingAttempt); // releases locks
      send(new ShardStoreReply(getResultOfTransaction(txnAttempt.amoTransaction())), client);

      // if no more ongoing transactions (as coordinator), re-process pending config
      if (!isSomeTxnOngoing() && this.pendingNewConfigCommand != null) {
        AMOCommand pendingConfig = this.pendingNewConfigCommand;
        this.pendingNewConfigCommand = null;
        this.newConfigSeenWhileTxnOngoing = false;
        processNewConfig(pendingConfig, true);
      }
    }
  }

  private void processTPCPrepareAbort(@NonNull AMOCommand amoCommand, boolean isReplicated) {
    TPCPrepareAbort tpcPrepareAbort = (TPCPrepareAbort) amoCommand.command();
    TransactionAttempt txnAttempt = tpcPrepareAbort.txnAttempt();
    Transaction transaction = (Transaction) txnAttempt.amoTransaction().command();

    if (!isReplicated) {
      handleMessage(new PaxosRequest(amoCommand), this.paxosAddress);
      return;
    }

    // replicated after this point:

    TransactionAttempt existingAttempt = getOngoingTxnAttemptAsCoord(txnAttempt.amoTransaction());
    if (existingAttempt == null) {
      // transaction no longer ongoing as coordinator
      return;
    }

    // can only accept PrepareAbort with matching retry number
    if (txnAttempt.retryNum() != existingAttempt.retryNum()) {
      return;
    }

    assertWithThrow(!isReconfigOngoing(), "S3.processTPCPrepareAbort: reconfig should not be ongoing");
    assertWithThrow(!isTxnAlreadyExecuted(txnAttempt.amoTransaction()), "S3.processTPCPrepareAbort: transaction should not already be executed");

    TxnCoordState coordState = this.transactionsOngoingAsCoord.get(existingAttempt);
    assertWithThrow(coordState.prepareOksReceived().size() < getTransactionParticipants(transaction, this.shardConfigLatest).size() - 1,
                    "S3.processTPCPrepareAbort: should not have received all PrepareOks yet");
    assertWithThrow(coordState.commitOksReceived().isEmpty(), "S3.processTPCPrepareAbort: CommitOks should be empty");

    if (coordState.isAborted()) {
      // already being handled, could also resend Abort here
      return;
    }

    // handle based on config number in PrepareAbort
    if (tpcPrepareAbort.configNum() < this.shardConfigLatest.configNum()) {
      // config number is lower, can't do much here
      return;
    } else if (tpcPrepareAbort.configNum() == this.shardConfigLatest.configNum()) {
      // config number matches, participant has locks for some other transaction
      coordState.setAborted(true);
      sendAbortToAllParticipants(existingAttempt, transaction);
    } else {
      // config number is higher, participant saw a new config
      this.newConfigSeenWhileTxnOngoing = true;
      coordState.setAborted(true);
      sendAbortToAllParticipants(existingAttempt, transaction);
    }
  }

  private void processTPCAbort(@NonNull AMOCommand amoCommand, boolean isReplicated) {
    TPCAbort tpcAbort = (TPCAbort) amoCommand.command();
    TransactionAttempt txnAttempt = tpcAbort.txnAttempt();
    Transaction transaction = (Transaction) txnAttempt.amoTransaction().command();

    if (!isReplicated) {
      handleMessage(new PaxosRequest(amoCommand), this.paxosAddress);
      return;
    }

    // replicated after this point:

    // assertWithThrow(!isTxnAlreadyExecuted(txnAttempt.amoTransaction()), "S3.processTPCAbort: transaction should not be already executed");
    // if transaction already executed, still send AbortOk to unblock coordinator
    if (isTxnAlreadyExecuted(txnAttempt.amoTransaction())) {
      sendToGroup(
          new ShardStoreTPCAbortOk(
              new TPCAbortOk(this.groupId, this.shardConfigLatest.configNum(), txnAttempt)
          ),
          tpcAbort.groupIdCoord()
      );
      return;
    }

    TransactionAttempt existingAttempt = getOngoingTxnAttemptAsPart(txnAttempt.amoTransaction());
    if (existingAttempt == null) {
      // locks already released, still send AbortOk in case the previous one was lost
      sendToGroup(
          new ShardStoreTPCAbortOk(
              new TPCAbortOk(this.groupId, this.shardConfigLatest.configNum(), txnAttempt)
          ),
          tpcAbort.groupIdCoord()
      );
      return;
    }

    if (txnAttempt.retryNum() != existingAttempt.retryNum()) {
      // stale abort, send AbortOk anyway
      sendToGroup(
          new ShardStoreTPCAbortOk(
              new TPCAbortOk(this.groupId, this.shardConfigLatest.configNum(), txnAttempt)
          ),
          tpcAbort.groupIdCoord()
      );
      return;
    }

    // release locks and send AbortOk
    this.transactionsOngoingAsPart.remove(existingAttempt);
    sendToGroup(
        new ShardStoreTPCAbortOk(
            new TPCAbortOk(this.groupId, this.shardConfigLatest.configNum(), existingAttempt)
        ),
        tpcAbort.groupIdCoord()
    );

    // if no more ongoing transactions (as participant), re-process pending config
    if (!isSomeTxnOngoing() && this.pendingNewConfigCommand != null) {
      AMOCommand pendingConfig = this.pendingNewConfigCommand;
      this.pendingNewConfigCommand = null;
      this.newConfigSeenWhileTxnOngoing = false;
      processNewConfig(pendingConfig, true);
    }
  }

  private void processTPCAbortOk(@NonNull AMOCommand amoCommand, boolean isReplicated) {
    TPCAbortOk tpcAbortOk = (TPCAbortOk) amoCommand.command();
    TransactionAttempt txnAttempt = tpcAbortOk.txnAttempt();
    Transaction transaction = (Transaction) txnAttempt.amoTransaction().command();

    if (!isReplicated) {
      handleMessage(new PaxosRequest(amoCommand), this.paxosAddress);
      return;
    }

    // replicated after this point:

    TransactionAttempt existingAttempt = getOngoingTxnAttemptAsCoord(txnAttempt.amoTransaction());
    if (existingAttempt == null) {
      // transaction no longer ongoing as coordinator
      return;
    }

    // config numbers do not matter here as a mismatch may have caused the abort
    TxnCoordState coordState = this.transactionsOngoingAsCoord.get(existingAttempt);
    // assertWithThrow(coordState.isAborted(), "S3.processTPCAbortOk: transaction should be marked as aborted");
    if (!coordState.isAborted()) {
      return;
    }
    // assertWithThrow(!isTxnAlreadyExecuted(txnAttempt.amoTransaction()), "S3.processTPCAbortOk: transaction should not already be executed");
    if (isTxnAlreadyExecuted(txnAttempt.amoTransaction())) {
      return;
    }
    assertWithThrow(!isReconfigOngoing(), "S3.processTPCAbortOk: reconfig should not be ongoing");

    HashSet<TPCAbortOk> abortOks = coordState.abortOksReceived();
    abortOks.add(tpcAbortOk);

    // once all participants have acknowledged the Abort
    if (abortOks.size() == getTransactionParticipants(transaction, this.shardConfigLatest).size() - 1) {
      if (this.newConfigSeenWhileTxnOngoing) {
        // drop the whole transaction, do not retry
        this.transactionsOngoingAsCoord.remove(existingAttempt);

        // if no more ongoing transactions, re-process the pending new config
        if (!isSomeTxnOngoing() && this.pendingNewConfigCommand != null) {
          AMOCommand pendingConfig = this.pendingNewConfigCommand;
          this.pendingNewConfigCommand = null;
          this.newConfigSeenWhileTxnOngoing = false;
          processNewConfig(pendingConfig, true);
        }
      } else {
        // retry the transaction with incremented retry number
        TransactionAttempt newAttempt = new TransactionAttempt(txnAttempt.amoTransaction(), existingAttempt.retryNum() + 1);
        this.transactionsOngoingAsCoord.remove(existingAttempt);
        this.transactionsOngoingAsCoord.put(newAttempt, new TxnCoordState());

        // send new Prepare to all participants
        sendAllExceptSelf(
            new ShardStoreTPCPrepare(
                new TPCPrepare(this.groupId, this.shardConfigLatest.configNum(), newAttempt)
            ),
            getTransactionParticipants(transaction, this.shardConfigLatest),
            true
        );
      }
    }
  }

  /* -----------------------------------------------------------------------------------------------
   *  Core Helpers
   * ---------------------------------------------------------------------------------------------*/

  // Update transactionsAlreadyExecuted to contain this transaction for the client which sent it
  private void setTxnAlreadyExecutedHelper(AMOCommand amoTransaction, KVStoreResult kvStoreResult) {
    assertWithThrow(!this.transactionsAlreadyExecuted.containsKey(amoTransaction.address())
            || this.transactionsAlreadyExecuted.get(amoTransaction.address()).amoCommand().sequenceNum() < amoTransaction.sequenceNum(),
        "S3.setTransactionsAlreadyExecutedHelper: transaction must be new (not already executed)");

    AMOResult amoResult = new AMOResult(kvStoreResult, amoTransaction.sequenceNum());
    this.transactionsAlreadyExecuted.put(amoTransaction.address(), new AMOExecution(amoTransaction, amoResult));
  }

  // Decomposes a transaction into the set of MultiCommands associated with the keys this group
  // manages, and executes the MultiCommands directly on the application (bypassing AMO logic).
  // The result of the transaction is then placed inside the transactionsAlreadyExecuted structure.
  //
  // Requires that either:
  //  1. The transaction is fully contained in a single group, and a Transaction decision was made
  //  2. The transaction is cross-group, and a COMMIT decision for an ongoing Transaction was received
  private void txnDecomposeAndExecute(AMOCommand amoTransaction, MultiGetResult valuesOfKeysInTxn) {
    assertWithThrow(amoTransaction.command() instanceof Transaction, "S3.txnDecomposeAndExecute: called on non-txn");
    Transaction transaction = (Transaction) amoTransaction.command();
    
    assertWithThrow(this.shardConfigLatest != null, "S3.txnDecomposeAndExecute: null config");
    assertWithThrow(!isTxnAlreadyExecuted(amoTransaction), "S3.txnDecomposeAndExecute: already executed txn");
    assertWithThrow(transaction.keySet().equals(valuesOfKeysInTxn.values().keySet()), "S3.txnDecomposeAndExecute: mismatch in keys of transaction and read values");


    assertWithThrow(getTransactionParticipants(transaction, this.shardConfigLatest).contains(this.groupId),
                    "S3.txnDecomposeAndExecute: executing txn, but not a participant in it");

    if (transaction instanceof MultiGet) {
      setTxnAlreadyExecutedHelper(amoTransaction, valuesOfKeysInTxn);
    } else if (transaction instanceof MultiPut multiPut) {

      for (String key : multiPut.keySet()) {
        if (this.amoApplicationSharded.containsKey(keyToShard(key))) {
          Application txnKVStoreShard = this.amoApplicationSharded.get(keyToShard(key)).application();

          HashMap<String, String> multiPutShard = new HashMap<>();
          multiPutShard.put(key, multiPut.values().get(key));
          txnKVStoreShard.execute(new MultiPut(multiPutShard));
        }
      }
      setTxnAlreadyExecutedHelper(amoTransaction, new MultiPutOk());

    } else if (transaction instanceof Swap swap) {

      if (this.amoApplicationSharded.containsKey(keyToShard(swap.key1())) && !valuesOfKeysInTxn.values().get(swap.key2()).equals(MultiGetResult.KEY_NOT_FOUND)) {
        Application txnKVStoreKey1 = this.amoApplicationSharded.get(keyToShard(swap.key1())).application();
        txnKVStoreKey1.execute(new Put(swap.key1(), valuesOfKeysInTxn.values().get(swap.key2())));
      }

      if (this.amoApplicationSharded.containsKey(keyToShard(swap.key2())) && !valuesOfKeysInTxn.values().get(swap.key1()).equals(MultiGetResult.KEY_NOT_FOUND)) {
        Application txnKVStoreKey2 = this.amoApplicationSharded.get(keyToShard(swap.key2())).application();
        txnKVStoreKey2.execute(new Put(swap.key2(), valuesOfKeysInTxn.values().get(swap.key1())));
      }

      setTxnAlreadyExecutedHelper(amoTransaction, new SwapOk());

    } else {
      assertWithThrow(false, "S3.txnDecomposeAndExecute: bad transaction");
    }
  }

  private AMOResult getResultOfTransaction(AMOCommand amoTransaction) {
    assertWithThrow(isTxnAlreadyExecuted(amoTransaction), "S3.getResultOfTransaction: must already be executed");
    AMOExecution amoExecutionLatest = this.transactionsAlreadyExecuted.get(amoTransaction.address());
    return amoExecutionLatest.amoResult();
  }

  // process commands that were queued during reconfiguration
  private void processRejectedCommands() {
    assertWithThrow(!isReconfigOngoing(), "S3.processRejectedCommands: reconfig still ongoing");

    while (!this.commandsRejectedDuringReconfig.isEmpty()) {
      AMOCommand amoCommand = this.commandsRejectedDuringReconfig.poll();
      process(amoCommand, true);
    }
  }

  // Given a new shard configuration, if the shards in this group have changed,
  // set up the reconfiguration data structures. It is assumed that:
  //   1. the new shard configuration has a config num exactly one larger than current config
  //      (have not "moved on" yet)
  //   2. reconfiguration is not already ongoing
  private void setupReconfigDS(ShardConfig shardConfigNew) {
    assertWithThrow(this.shardConfigLatest != null,  "S3.setupReconfigDS: null latest config");
    assertWithThrow(shardConfigNew.configNum() == this.shardConfigLatest.configNum() + 1, "S3.setupReconfigDS: config in new should be one larger");
    assertWithThrow(!isReconfigOngoing(), "S3.setupReconfigDS: reconfig ongoing but decision for another reconfig being processed");

    Set<Integer> shardsThisGroupOldConfig = getShards(this.shardConfigLatest, this.groupId);
    Set<Integer> shardsThisGroupNewConfig = getShards(shardConfigNew, this.groupId);

    if (shardsThisGroupOldConfig.size() == shardsThisGroupNewConfig.size()) {
      // unchanged, don't do anything
      assertWithThrow(shardsThisGroupOldConfig.equals(shardsThisGroupNewConfig),
                      "S3.setupReconfigDS: equal number of shards but shards were moved (suboptimal)");
    }
    else if (shardsThisGroupOldConfig.size() > shardsThisGroupNewConfig.size()) {
      // losing shards
      assertWithThrow(shardsThisGroupOldConfig.containsAll(shardsThisGroupNewConfig),
                      "S3.setupReconfigDS: losing shards but also gained new ones (suboptimal)");

      constructReconfigDecisionsNeeded(this.shardConfigLatest, shardConfigNew);
      assertWithThrow(!this.reconfigAcksNeeded.isEmpty(), "S3.setupReconfigDS: acks needed empty (should be non-empty)");

      resendShardMoves(shardConfigNew);
      set(new ResendShardMovesTimer(shardConfigNew.configNum()), ResendShardMovesTimer.RESEND_MILLIS);
    }
    else {
      // gaining shards
      assertWithThrow(shardsThisGroupNewConfig.containsAll(shardsThisGroupOldConfig),
                      "S3.setupReconfigDS: gaining shards but lost original ones (suboptimal)");

      constructReconfigDecisionsNeeded(this.shardConfigLatest, shardConfigNew);
      assertWithThrow(!this.reconfigMovesNeeded.isEmpty(), "S3.setupReconfigDS: moves needed empty (should be non-empty)");
    }
  }

  // will construct the ReconfigMovesNeeded and ReconfigAcksNeeded for this server
  // by comparing the difference in shard management between the two configurations passed in
  private void constructReconfigDecisionsNeeded(@NonNull ShardConfig shardConfigOld, @NonNull ShardConfig shardConfigNew) {
    assertWithThrow(this.reconfigAcksNeeded.isEmpty() && this.reconfigMovesNeeded.isEmpty(),
                    "S3.constructReconfigDecisionsNeeded: group should have empty reconfig data structures");
    assertWithThrow(shardConfigOld.configNum() + 1 == shardConfigNew.configNum(),
                    "S3.constructReconfigDecisionsNeeded: configs must be one apart");

    for (int shard = ShardMaster.SHARD_NUM_START; shard <= numTotalShards; shard++) {
      int groupMngShardOldConfig = getGroupIdForShard(shardConfigOld, shard);
      int groupMngShardNewConfig = getGroupIdForShard(shardConfigNew, shard);

      // this group is losing a shard
      if (groupMngShardOldConfig == this.groupId && groupMngShardNewConfig != this.groupId) {
        this.reconfigAcksNeeded.putIfAbsent(groupMngShardNewConfig, new HashSet<>());
        this.reconfigAcksNeeded.get(groupMngShardNewConfig).add(shard);
      }

      // this group is gaining a shard
      if (groupMngShardOldConfig != this.groupId && groupMngShardNewConfig == this.groupId) {
        this.reconfigMovesNeeded.putIfAbsent(groupMngShardOldConfig, new HashSet<>());
        this.reconfigMovesNeeded.get(groupMngShardOldConfig).add(shard);
      }
    }

    assertWithThrow(this.reconfigAcksNeeded.isEmpty() || this.reconfigMovesNeeded.isEmpty(),
                    "S3.constructReconfigDecisionsNeeded: group should not both gain and lose shards");
  }

  // while reconfiguration is ongoing, and this group is sending shards + waiting to receive acks,
  // resend the shards to each of the groups that this group is waiting for an ack from
  private void resendShardMoves(@NonNull ShardConfig shardConfig) {
    assertWithThrow(isReconfigOngoing(), "S3.resendShardMoves: resending but reconfig not ongoing");
    assertWithThrow(!this.reconfigAcksNeeded.isEmpty(), "S3.resendShardMoves: acks empty (but resending)");

    for (Integer groupIdReceiver : this.reconfigAcksNeeded.keySet()) {
      assertWithThrow(shardConfig.groupInfo().containsKey(groupIdReceiver), "resendShardMoves: receiver not in shardConfig");

      // tell receiver that this group is sending a collection of application shards to them
      ShardMove shardMoveToReceiver = new ShardMove(this.groupId, shardConfig.configNum(), new HashMap<>(), this.transactionsAlreadyExecuted);

      this.reconfigAcksNeeded.get(groupIdReceiver).forEach((shard) -> {
        shardMoveToReceiver.amoAppShards().put(shard, this.amoApplicationSharded.get(shard));
      });

      broadcast(
          new ShardStoreShardMove(shardMoveToReceiver),
          getServersForGroupId(shardConfig, groupIdReceiver)
      );
    }
  }

  // for 2PC:

  // Get the values of all keys in the transaction that this group manages
  // (could still return KEY_NOT_FOUND for some keys).
  // Requires that this group has the locks acquired for the transaction, or is
  // the only group involved in the transaction.
  private MultiGetResult getValuesOfKeysManagedInTxn(@NonNull AMOCommand amoTransaction) {
    assertWithThrow(amoTransaction.command() instanceof Transaction, "S3.getValuesOfKeysInTxn: Bad amo command");
    Transaction transaction = (Transaction) amoTransaction.command();

    assertWithThrow( getTransactionParticipants(transaction, this.shardConfigLatest).size() == 1
                     || locksAcquiredAsParticipant(amoTransaction)
                     || locksAcquiredAsCoordinator(amoTransaction),
        "S3.getValuesOfKeysInTxn: Must have the locks acquired for txn already (unless txn is not cross-group)");

    MultiGetResult multiGetResultAggregated = new MultiGetResult(new HashMap<>());

    // bypass AMO logic to run MultiGet on the various transactional KVStore shards this group manages
    transaction.keySet().forEach(key -> {
      if (this.amoApplicationSharded.containsKey(keyToShard(key))) {
        Application txnKVStoreShard = this.amoApplicationSharded.get(keyToShard(key)).application();
        MultiGetResult multiGetResult = (MultiGetResult) txnKVStoreShard.execute(
            new MultiGet(new HashSet<>(Collections.singleton(key)))
        );
        multiGetResultAggregated.values().putAll(multiGetResult.values());
      }
    });

    return multiGetResultAggregated;
  }

  /* -----------------------------------------------------------------------------------------------
   *  Timer Handlers
   * ---------------------------------------------------------------------------------------------*/
  private synchronized void onQueryTimer(QueryTimer t) {
    sendQueryShardMasters();
    set(t, QueryTimer.QUERY_RETRY_MILLIS);
  }

  private synchronized void onResendShardMovesTimer(ResendShardMovesTimer t) {
    assertWithThrow(this.shardConfigLatest != null && this.shardConfigLatest.configNum() >= t.configNum(),
                    "S3.onResendShardMovesTimer: this server's config num is incorrectly behind timer");

    if (t.configNum() < this.shardConfigLatest.configNum() || !isReconfigOngoing()) {
      return;
    }

    assertWithThrow(!this.reconfigAcksNeeded.isEmpty(), "S3.onResendShardMovesTimer: reconfig ongoing but acks empty");
    resendShardMoves(this.shardConfigLatest);
    set(t, ResendShardMovesTimer.RESEND_MILLIS);
  }

  private synchronized void onDriveOngoingTransactionsTimer(DriveOngoingTransactionsTimer t) {
    if (this.shardConfigLatest == null || isReconfigOngoing()) {
      set(t, DriveOngoingTransactionsTimer.DRIVE_TXN_MILLIS);
      return;
    }

    // for each ongoing transaction where this group is the coordinator
    for (TransactionAttempt txnAttempt : this.transactionsOngoingAsCoord.keySet()) {
      TxnCoordState coordState = this.transactionsOngoingAsCoord.get(txnAttempt);
      Transaction transaction = (Transaction) txnAttempt.amoTransaction().command();
      int numParticipants = getTransactionParticipants(transaction, this.shardConfigLatest).size();

      if (coordState.isAborted()) {
        // send abort to all participants
        sendAbortToAllParticipants(txnAttempt, transaction);
      } else if (coordState.prepareOksReceived().size() < numParticipants - 1) {
        // not all PrepareOks received, resend Prepare to all participants
        sendAllExceptSelf(
            new ShardStoreTPCPrepare(
                new TPCPrepare(this.groupId, this.shardConfigLatest.configNum(), txnAttempt)
            ),
            getTransactionParticipants(transaction, this.shardConfigLatest),
            true
        );
      } else if (coordState.commitOksReceived().size() < numParticipants - 1) {
        // all PrepareOks received but not all CommitOks, resend Commit
        // need to reconstruct the merged values for the Commit message
        MultiGetResult valuesOfKeysMerged = getValuesOfKeysManagedInTxn(txnAttempt.amoTransaction());
        for (TPCPrepareOk prepareOkReceived : coordState.prepareOksReceived()) {
          valuesOfKeysMerged.values().putAll(prepareOkReceived.valuesOfTxnKeys().values());
        }
        sendAllExceptSelf(
            new ShardStoreTPCCommit(
                new TPCCommit(this.groupId, this.shardConfigLatest.configNum(), txnAttempt, valuesOfKeysMerged)
            ),
            getTransactionParticipants(transaction, this.shardConfigLatest),
            true
        );
      }
    }

    set(t, DriveOngoingTransactionsTimer.DRIVE_TXN_MILLIS);
  }

  /* -----------------------------------------------------------------------------------------------
   *  Utils
   * ---------------------------------------------------------------------------------------------*/

  private boolean isTxnAlreadyExecuted(@NonNull AMOCommand amoTransaction) {
    assertWithThrow(amoTransaction.command() instanceof Transaction, "S3.isTxnAlreadyExecuted: cmd not txn");

    if (!this.transactionsAlreadyExecuted.containsKey(amoTransaction.address())) {
      return false;
    }

    // transaction is already executed if the highest executed seq num
    // is at least as large as the seq num in the command
    return this.transactionsAlreadyExecuted.get(
        amoTransaction.address()
    ).amoResult().sequenceNum() >= amoTransaction.sequenceNum();
  }

  // returns the set of shards that the group in the argument manages, or returns emptyset
  // if the group in the argument is not in the configuration passed in
  private Set<Integer> getShards(@NonNull ShardConfig shardConfig, int groupIdGetShards) {
    if (!shardConfig.groupInfo().containsKey(groupIdGetShards)) {
      return new HashSet<>();
    }
    return shardConfig.groupInfo().get(groupIdGetShards).getRight();
  }

  private boolean isNoOverlapInTxnKeySetCoord(AMOCommand amoCommand, Set<TransactionAttempt> txnAttemptsWithLocksAcq) {
    assertWithThrow(amoCommand.command() instanceof Transaction, "S3.isNoOverlapInTxnKeySetCoord: command must be txn");
    // assertWithThrow(txnAttemptsWithLocksAcq.stream().allMatch(t -> t.amoTransaction().command() instanceof Transaction), "S3.isNoOverlapInTxnKeySet: commands in transaction set must be transactions");

    Transaction transaction = (Transaction) amoCommand.command();

    return txnAttemptsWithLocksAcq.stream().allMatch(txnAttemptLocked -> {
      Transaction transactionLockAcq = (Transaction) txnAttemptLocked.amoTransaction().command();
      return Sets.intersection(transaction.keySet(), transactionLockAcq.keySet()).isEmpty();
    });
  }

  private boolean isNoOverlapInTxnKeySetPart(AMOCommand amoCommand, Set<TransactionAttempt> txnAttemptsWithLocksAcq) {
    assertWithThrow(amoCommand.command() instanceof Transaction, "S3.isNoOverlapInTxnKeySetPart: command must be txn");
    // assertWithThrow(txnAttemptsWithLocksAcq.stream().allMatch(t -> t.amoTransaction().command() instanceof Transaction), "S3.isNoOverlapInTxnKeySet: commands in transaction set must be transactions");

    Transaction transaction = (Transaction) amoCommand.command();

    return txnAttemptsWithLocksAcq.stream().allMatch(txnAttemptLocked -> {
      Transaction transactionLockAcq = (Transaction) txnAttemptLocked.amoTransaction().command();
      return Sets.intersection(transaction.keySet(), transactionLockAcq.keySet()).isEmpty();
    });
  }

  // should only be called if the transaction is not already ongoing,
  // either as the coordinator or participant
  private boolean canAcquireLocks(@NonNull AMOCommand amoTransaction) {
    assertWithThrow(getOngoingTxnAttemptAsCoord(amoTransaction) == null && getOngoingTxnAttemptAsPart(amoTransaction) == null,
                    "S3.canAcquireLocks: locks already acquired for transaction, should not check again");

    return isNoOverlapInTxnKeySetCoord(amoTransaction, this.transactionsOngoingAsCoord.keySet())
        && isNoOverlapInTxnKeySetPart(amoTransaction, this.transactionsOngoingAsPart);
  }

  // returns the TransactionAttempt if the transaction is ongoing as participant, otherwise null
  private TransactionAttempt getOngoingTxnAttemptAsPart(AMOCommand amoTransaction) {
    assertWithThrow(amoTransaction.command() instanceof Transaction, "S3.getOngoingTxnAttemptAsPart: Bad amo command");
    TransactionAttempt lookupAttempt = new TransactionAttempt(amoTransaction, 0); // retry num doesn't matter for lookup
    for (TransactionAttempt attempt : this.transactionsOngoingAsPart) {
      if (attempt.equals(lookupAttempt)) {
        return attempt;
      }
    }
    return null;
  }

  // returns the TransactionAttempt if the transaction is ongoing as coordinator, otherwise null
  private TransactionAttempt getOngoingTxnAttemptAsCoord(AMOCommand amoTransaction) {
    assertWithThrow(amoTransaction.command() instanceof Transaction, "S3.getOngoingTxnAttemptAsCoord: Bad amo command");
    TransactionAttempt lookupAttempt = new TransactionAttempt(amoTransaction, 0); // retry num doesn't matter for lookup
    for (TransactionAttempt attempt : this.transactionsOngoingAsCoord.keySet()) {
      if (attempt.equals(lookupAttempt)) {
        return attempt;
      }
    }
    return null;
  }

  private boolean locksAcquiredAsParticipant(AMOCommand amoTransaction) {
    // assertWithThrow(amoTransaction.command() instanceof Transaction, "S3.locksAcquiredAsParticipant: Bad amo command");
    // assertWithThrow(getTransactionParticipants(transaction, this.shardConfigLatest).contains(this.groupId), "S3.locksAcquiredAsParticipant: this group must be involved in transaction");
    return getOngoingTxnAttemptAsPart(amoTransaction) != null;
  }

  private boolean locksAcquiredAsCoordinator(AMOCommand amoTransaction) {
    // assertWithThrow(amoTransaction.command() instanceof Transaction, "S3.locksAcquiredAsCoordinator: Bad amo command");
    // assertWithThrow(getTransactionParticipants(transaction, this.shardConfigLatest).contains(this.groupId), "S3.locksAcquiredAsCoordinator: this group must be involved in transaction");
    return getOngoingTxnAttemptAsCoord(amoTransaction) != null;
  }

  // send Abort to all participants for the given transaction
  private void sendAbortToAllParticipants(TransactionAttempt txnAttempt, Transaction transaction) {
    sendAllExceptSelf(
        new ShardStoreTPCAbort(
            new TPCAbort(this.groupId, this.shardConfigLatest.configNum(), txnAttempt)
        ),
        getTransactionParticipants(transaction, this.shardConfigLatest),
        true
    );
  }

  // this group is a coordinator or pure participant in at least one transaction
  private boolean isSomeTxnOngoing() {
    return !(this.transactionsOngoingAsCoord.isEmpty() && this.transactionsOngoingAsPart.isEmpty());
  }

  private boolean isReconfigOngoing() {
    return (this.reconfigMovesNeeded.size() + this.reconfigAcksNeeded.size()) > 0;
  }

  private boolean isManagingCommand(Command command) {
    if (this.shardConfigLatest == null) {
      return false;
    }

    return this.groupId == computeGroupManagingCommand(command, this.shardConfigLatest);
  }

  private void sendToGroup(Message message, int groupIdReceiver) {
    assertWithThrow(this.groupId != groupIdReceiver, "S3.sendToGroup: should not send to self");
    assertWithThrow(this.shardConfigLatest != null && this.shardConfigLatest.groupInfo().containsKey(groupIdReceiver),
        "S3.sendToGroup: cannot send to non-existent group");
    broadcast(message, this.shardConfigLatest.groupInfo().get(groupIdReceiver).getLeft());
  }

  // Send a message to all groups that need the message, but don't send to self.
  // It is required that every group in the set is in the current config,
  // and the group set contains this group as part of it
  // (The isCoordinator argument is just meant to make you think about when this function is called (participants should not call this))
  // TODO: probably should enforce isCoordinator in a better way
  private void sendAllExceptSelf(Message message, Set<Integer> groupIds, boolean isCoordinator) {
    assertWithThrow(isCoordinator, "S3.sendAllExceptSelf: only coordinator for a transaction should be broadcasting across groups");
    assertWithThrow(groupIds.contains(this.groupId), "S3.sendAllExceptSelf: this group should be in groupset");
    groupIds.forEach(groupIdReceiver -> {
      if (groupIdReceiver != this.groupId) {
        sendToGroup(message, groupIdReceiver);
      }
    });
  }

  private AMOCommand wrapInDummyAMO(Command command) {
    assertWithThrow(!(command instanceof AMOCommand), "S3.wrapInDummyAMO: wrapping command already an AMO");
    return new AMOCommand(command, null, SEQNUM_DONTCARE);
  }

  private void sendQueryShardMasters() {
    Query query = new Query(getConfigNumForQuery());
    // use config number as sequence number
    broadcast(
        new PaxosRequest(new AMOCommand(query, this.address(), SEQNUM_DONTCARE)),
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

  public void assertWithThrow(boolean b, String m) {
    if (!b) {
      new Exception().printStackTrace(System.out);
      System.out.println(m);
      System.exit(1);
    }
  }
}
