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
  private final HashMap<AMOCommand, Pair<HashSet<TPCPrepareOk>, HashSet<TPCCommitOk>>> transactionsOngoingAsCoord;
  private final HashSet<AMOCommand> transactionsOngoingAsPart;

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
  }

  @Data
  public static final class ShardMoveAck implements Command {
    private final int groupIdReceiver; // the group that received the shards (the group that gets this message is the original sender of the shards)
    private final int configNum;
    private final Map<Integer, AMOApplication<Application>> amoAppShards;
  }

  @Data
  public static final class TPCPrepare implements Command {
    // TODO: add in retry number somewhere
    private final int groupIdCoord;
    private final int configNum;
    private final AMOCommand amoTransaction; // contains transaction

    public TPCPrepare(int groupIdCoord, int configNum, AMOCommand amoTransaction) {
      if (!(amoTransaction.command() instanceof Transaction)) {
        // TODO: assertWithThrow() (so we remember to remove this)
        System.out.println("S3.TPCPrepare: Bad amo command " + amoTransaction.command());
        System.exit(255);
      }
      this.groupIdCoord = groupIdCoord;
      this.configNum = configNum;
      this.amoTransaction = amoTransaction;
    }
  }

  @Data
  public static final class TPCPrepareOk implements Command {
    // TODO: add in retry number somewhere
    // TODO: add in values
    private final int groupIdPart; //
    private final int configNum;
    private final AMOCommand amoTransaction; // contains transaction

    public TPCPrepareOk(int groupIdPart, int configNum, AMOCommand amoTransaction) {
      if (!(amoTransaction.command() instanceof Transaction)) {
        // TODO: assertWithThrow() (so we remember to remove this)
        System.out.println("S3.TPCPrepareOk: Bad amo command " + amoTransaction.command());
        System.exit(255);
      }
      this.groupIdPart = groupIdPart;
      this.configNum = configNum;
      this.amoTransaction = amoTransaction;
    }
  }

  @Data
  public static final class TPCCommit implements Command {
    // TODO:
  }

  @Data
  public static final class TPCCommitOk implements Command {
    // TODO:
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
      assertWithThrow(!isSomeTxnOngoing(),
                      "S3.processNewConfig: handle case where new config comes but txn ongoing");

      // new configuration after initial case
      assertWithThrow(shardConfigNew.configNum() == this.shardConfigLatest.configNum() + 1,
                      "S3.processNewConfig: decision for larger config must be exactly one higher");
      setupReconfigDS(shardConfigNew);
    }

    // take on the new configuration (which must be higher)
    this.shardConfigLatest = shardConfigNew;
  }

  private void processTransaction(@NonNull AMOCommand amoCommand, boolean isReplicated) {
    assertWithThrow(amoCommand.command() instanceof Transaction, "S3.processTransaction: called with wrong command type");

    if (isReconfigOngoing()) {
      assertWithThrow(!isSomeTxnOngoing(), "S3.processTransaction: no txn should be ongoing during reconfig");
      // TODO: think about this more
      // if (isReplicated) { this.commandsRejectedDuringReconfig.add(amoCommand); }
      return;
    }

    // check if this group is managing the transaction (coordinator)
    Transaction transaction = (Transaction) amoCommand.command();
    if (!isManagingCommand(transaction)) {
      return;
    }

    // if the transaction has already been executed, then reply back to the client
    if (isTxnAlreadyExecuted(amoCommand)) {
      AMOExecution amoExecutionLatest = this.transactionsAlreadyExecuted.get(amoCommand.address());
      send(new ShardStoreReply(amoExecutionLatest.amoResult()), amoCommand.address());
      return;
    }

    // TODO: check that no transaction that references the same key(s) are ongoing (should also handle if the transaction is already ongoing)
    assertWithThrow(canAcquireLocks(amoCommand), "S3.processTransaction: handle case where locks already acquired");

    // transaction not handled before, either propose or execute
    if (!isReplicated) {
      handleMessage(new PaxosRequest(amoCommand), this.paxosAddress);
    } else {

      if (getTransactionParticipants(transaction, this.shardConfigLatest).size() == 1) {
        // this group can execute the entire transaction at once (no 2PC case)
        KVStoreResult kvStoreResult = transactionDecomposeAndExecute(amoCommand);
        AMOResult amoResult = new AMOResult(kvStoreResult, amoCommand.sequenceNum());
        this.transactionsAlreadyExecuted.put(amoCommand.address(), new AMOExecution(amoCommand, amoResult));

        send(new ShardStoreReply(amoResult), amoCommand.address());
      } else {
        // 2PC case, this group is the coordinator, setup DS, and send prepares to all participants
        assertWithThrow(!this.transactionsOngoingAsCoord.containsKey(amoCommand), "S3.processTransaction: starting new txn but it's already ongoing");

        this.transactionsOngoingAsCoord.put(amoCommand, new ImmutablePair<>(new HashSet<>(), new HashSet<>()));
        sendAllExceptSelf(
            new ShardStoreTPCPrepare(
                new TPCPrepare(this.groupId, this.shardConfigLatest.configNum(), amoCommand)
            ),
            getTransactionParticipants(transaction, this.shardConfigLatest)
        );
      }
    }
  }

  private void processTPCPrepare(@NonNull AMOCommand amoCommand, boolean isReplicated) {
    TPCPrepare tpcPrepare = (TPCPrepare) amoCommand.command();
    Transaction transaction = (Transaction) tpcPrepare.amoTransaction().command();

    assertWithThrow(!isReconfigOngoing(), "S3.processTPCPrepare: reconfig ongoing case (send abort)");
    assertWithThrow(this.shardConfigLatest != null && tpcPrepare.configNum() == this.shardConfigLatest.configNum(), "S3.processTPCPrepare: config mismatch (send abort)");
    assertWithThrow(!isTxnAlreadyExecuted(tpcPrepare.amoTransaction()), "S3.processTPCPrepare: participant already executed transaction (can drop)");
    assertWithThrow(!isManagingCommand(tpcPrepare.amoTransaction().command()), "S3.processTPCPrepare: somehow got prepare when managing command in same config as sender (BAD)");

    // the Prepare to eventually send back to coordinator (is unused if not replicated)
    ShardStoreTPCPrepareOk tpcPrepareOk = new ShardStoreTPCPrepareOk(
        new TPCPrepareOk(this.groupId, this.shardConfigLatest.configNum(), tpcPrepare.amoTransaction())
    );

    if (this.transactionsOngoingAsPart.contains(tpcPrepare.amoTransaction())) {
      // duplicated prepare, send back PrepareOk (works because the config num is the same)
      sendAllExceptSelf(tpcPrepareOk, getTransactionParticipants(transaction, this.shardConfigLatest));
      return;
    }

    assertWithThrow(canAcquireLocks(tpcPrepare.amoTransaction()), "S3.processTPCPrepare: locks cannot be acquired (abort)");

    if (!isReplicated) {
      handleMessage(new PaxosRequest(amoCommand), this.paxosAddress);
    } else {
      // TODO: also send values associated with keys in transaction
      this.transactionsOngoingAsPart.add(tpcPrepare.amoTransaction());
      sendAllExceptSelf(tpcPrepareOk, getTransactionParticipants(transaction, this.shardConfigLatest));
    }
  }

  private void processTPCPrepareOk(@NonNull AMOCommand amoCommand, boolean isReplicated) {
    TPCPrepareOk tpcPrepareOk = (TPCPrepareOk) amoCommand.command();
    Transaction transaction = (Transaction) tpcPrepareOk.amoTransaction().command();

    assertWithThrow(!isReconfigOngoing(), "S3.processTPCPrepareOk: reconfig ongoing (config nums should not match then)");
    assertWithThrow(tpcPrepareOk.configNum() == this.shardConfigLatest.configNum(), "S3.processTPCPrepareOk: config num mismatch (just drop, the participant may not even have locks acquired anymore)");
    // TODO: should make amoTransaction a separate type (so that we don't screw things up accidentally)
    assertWithThrow(this.transactionsOngoingAsCoord.containsKey(tpcPrepareOk.amoTransaction()), "S3.processTPCPrepareOk: transaction no longer ongoing (can drop)");
    assertWithThrow(isManagingCommand(transaction), "S3.processTPCPrepareOk: got prepare ok but not manager (coordinator) of transaction");

    if (!isReplicated) {
      handleMessage(new PaxosRequest(amoCommand), this.paxosAddress);
    } else {
      HashSet<TPCPrepareOk> prepareOks = this.transactionsOngoingAsCoord.get(tpcPrepareOk.amoTransaction()).getLeft();
      prepareOks.add(tpcPrepareOk);

      // once every other group has sent back a PrepareOk, can COMMIT
      if (prepareOks.size() == getTransactionParticipants(transaction, this.shardConfigLatest).size() - 1) {
        // TODO: aggregate values, and read own keys here as well
        assertWithThrow(false, "2PC COMMIT WILL COMMENCE!!");
      }
    }
  }

  /* -----------------------------------------------------------------------------------------------
   *  Core Helpers
   * ---------------------------------------------------------------------------------------------*/

  // Decomposes a transaction into the set of MultiCommands associated with the keys this group
  // manages, and executes the MultiCommands directly on the application (bypassing AMO logic).
  // The aggregated results of the MultiCommands are returned as a single Transaction KVStoreResult
  //
  // Requires that either:
  //  1. The transaction is fully contained in a single group, and a Transaction decision was made
  //  2. The transaction is cross-group, and a COMMIT decision for an ongoing Transaction was received
  private KVStoreResult transactionDecomposeAndExecute(@NonNull AMOCommand amoCommand) {
    assertWithThrow(amoCommand.command() instanceof Transaction, "S3.transactionDecomposeAndExecute: called on non-txn");
    assertWithThrow(this.shardConfigLatest != null, "S3.transactionDecomposeAndExecute: null config");
    assertWithThrow(!isTxnAlreadyExecuted(amoCommand), "S3.transactionDecomposeAndExecute: already executed txn");

    Transaction transaction = (Transaction) amoCommand.command();
    assertWithThrow(getTransactionParticipants(transaction, this.shardConfigLatest).contains(this.groupId),
                    "S3.transactionDecomposeAndExecute: executing txn, but not a participant in it");
    assertWithThrow(transaction.keySet().stream().allMatch(key -> this.amoApplicationSharded.containsKey(keyToShard(key))),
                    "S3.transactionDecomposeAndExecute: have not yet handled managing subset of keys");

    if (transaction instanceof MultiGet multiGet) {
      MultiGetResult multiGetResultAggregated = new MultiGetResult(new HashMap<>());

      // bypass AMO logic to run MultiGet on the various transactional KVStore shards this group manages
      for (String key : multiGet.keySet()) {
        Application txnKVStoreShard = this.amoApplicationSharded.get(keyToShard(key)).application();
        MultiGetResult multiGetResult = (MultiGetResult) txnKVStoreShard.execute(
            new MultiGet(new HashSet<>(Collections.singleton(key)))
        );
        multiGetResultAggregated.values().putAll(multiGetResult.values());
      }

      return multiGetResultAggregated;

    } else if (transaction instanceof MultiPut multiPut) {

      for (String key : multiPut.keySet()) {
        Application txnKVStoreShard = this.amoApplicationSharded.get(keyToShard(key)).application();

        HashMap<String, String> multiPutShard = new HashMap<>();
        multiPutShard.put(key, multiPut.values().get(key));
        txnKVStoreShard.execute(new MultiPut(multiPutShard));
      }

      return new MultiPutOk();

    } else if (transaction instanceof Swap swap) {
      // TODO: need to fix this for cross-group swaps
      if (keyToShard(swap.key1()) == keyToShard(swap.key2())) {
        assertWithThrow(false, "S3.transactionDecomposeAndExecute: txn on same shard (not tested yet)");
        // this.amoApplicationSharded.get(keyToShard(swap.key1())).application().execute(swap);
      } else {
        // swap across shards
        Application txnKVStoreKey1 = this.amoApplicationSharded.get(keyToShard(swap.key1())).application();
        Result resultKey1Get = txnKVStoreKey1.execute(new Get(swap.key1()));

        Application txnKVStoreKey2 = this.amoApplicationSharded.get(keyToShard(swap.key2())).application();
        Result resultKey2Get = txnKVStoreKey2.execute(new Get(swap.key2()));

        // TODO: this implementation does not completely match the swap in TransactionalKVStore.java (no keys are deleted here)
        if (resultKey1Get instanceof GetResult getResult) {
          txnKVStoreKey2.execute(new Put(swap.key2(), getResult.value()));
        }
        if (resultKey2Get instanceof GetResult getResult) {
          txnKVStoreKey1.execute(new Put(swap.key1(), getResult.value()));
        }
      }

      return new SwapOk();
    } else {
      assertWithThrow(false, "S3.transactionDecomposeAndExecute: bad transaction");
      return null;
    }
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
      ShardMove shardMoveToReceiver = new ShardMove(this.groupId, shardConfig.configNum(), new HashMap<>());

      this.reconfigAcksNeeded.get(groupIdReceiver).forEach((shard) -> {
        shardMoveToReceiver.amoAppShards().put(shard, this.amoApplicationSharded.get(shard));
      });

      broadcast(
          new ShardStoreShardMove(shardMoveToReceiver),
          getServersForGroupId(shardConfig, groupIdReceiver)
      );
    }
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

  /* -----------------------------------------------------------------------------------------------
   *  Utils
   * ---------------------------------------------------------------------------------------------*/

  private boolean isTxnAlreadyExecuted(@NonNull AMOCommand amoCommand) {
    assertWithThrow(amoCommand.command() instanceof Transaction, "S3.isTxnAlreadyExecuted: cmd not txn");

    if (!this.transactionsAlreadyExecuted.containsKey(amoCommand.address())) {
      return false;
    }

    // transaction is already executed if the highest executed seq num
    // is at least as large as the seq num in the command
    return this.transactionsAlreadyExecuted.get(
        amoCommand.address()
    ).amoResult().sequenceNum() >= amoCommand.sequenceNum();
  }

  // returns the set of shards that the group in the argument manages, or returns emptyset
  // if the group in the argument is not in the configuration passed in
  private Set<Integer> getShards(@NonNull ShardConfig shardConfig, int groupIdGetShards) {
    if (!shardConfig.groupInfo().containsKey(groupIdGetShards)) {
      return new HashSet<>();
    }
    return shardConfig.groupInfo().get(groupIdGetShards).getRight();
  }

  private boolean isNoOverlapInTxnKeySet(AMOCommand amoCommand, Set<AMOCommand> amoCommandsWithLocksAcq) {
    assertWithThrow(amoCommand.command() instanceof Transaction, "S3.isNoOverlapInTxnKeySet: command must be txn");
    assertWithThrow(amoCommandsWithLocksAcq.stream().allMatch(amoCommandLocked -> amoCommandLocked.command() instanceof Transaction),
                  "S3.isNoOverlapInTxnKeySet: commands in transaction set must be transactions");

    Transaction transaction = (Transaction) amoCommand.command();

    return amoCommandsWithLocksAcq.stream().allMatch(amoCommandLocked -> {
      Transaction transactionLockAcq = (Transaction) amoCommandLocked.command();
      return Sets.intersection(transaction.keySet(), transactionLockAcq.keySet()).isEmpty();
    });
  }

  // should only be called if the transaction is not already ongoing,
  // either as the coordinator or participant
  private boolean canAcquireLocks(@NonNull AMOCommand amoTransaction) {
    assertWithThrow(!this.transactionsOngoingAsCoord.containsKey(amoTransaction) && !this.transactionsOngoingAsPart.contains(amoTransaction),
                    "S3.canAcquireLocks: locks already acquired for transaction, should not check again");

    return isNoOverlapInTxnKeySet(amoTransaction, this.transactionsOngoingAsCoord.keySet())
        && isNoOverlapInTxnKeySet(amoTransaction, this.transactionsOngoingAsPart);
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

  // Send a message to all groups that need the message, but don't send to self.
  // It is required that every group in the set is in the current config,
  // and the group set contains this group as part of it
  private void sendAllExceptSelf(Message message, Set<Integer> groupIds) {
    assertWithThrow(groupIds.contains(this.groupId), "S3.sendAllExceptSelf(group version): this group should be in groupset");

    for (Integer groupIdToSendTo : groupIds) {
      assertWithThrow(this.shardConfigLatest != null && this.shardConfigLatest.groupInfo().containsKey(groupIdToSendTo),
                      "S3.sendAllExceptSelf(group version): every group ");

      if (groupIdToSendTo != this.groupId) {
        broadcast(message, this.shardConfigLatest.groupInfo().get(groupIdToSendTo).getLeft());
      }
    }
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
      System.out.println(m);
      System.exit(1);
    }
  }
}
