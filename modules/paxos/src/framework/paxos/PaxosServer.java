package framework.paxos;

import framework.atmostonce.AMOApplication;
import framework.atmostonce.AMOCommand;
import framework.atmostonce.AMOResult;
import framework.Address;
import framework.Application;
import framework.Command;
import framework.Message;
import framework.Node;
import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NonNull;
import lombok.ToString;

@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public class PaxosServer extends Node {
  /** All servers in the Paxos group, including this one. */
  private final Address[] servers;
  private final AMOApplication<Application> amoApplication;

  private static final int LOG_START = 1;
  private static final int LOG_UNKNOWN = 0;

  @Data
  public static class Ballot implements Comparable<Ballot>, Serializable {
    private int sequenceNum;
    private Address address;

    public Ballot(int sequenceNum, Address address) {
      this.sequenceNum = sequenceNum;
      this.address = address;
    }
    @Override
    public int compareTo(Ballot ballotCmpTo) {
      if (this.sequenceNum != ballotCmpTo.sequenceNum()) { return this.sequenceNum - ballotCmpTo.sequenceNum(); }
      return this.address.compareTo(ballotCmpTo.address());
    }
  }

  @Data
  public static class LogEntry implements Serializable {
    private AMOCommand amoCommand;
    private Ballot ballot;
    private PaxosLogSlotStatus status;

    public LogEntry(AMOCommand amoCommand, Ballot ballot, PaxosLogSlotStatus status) {
      this.amoCommand = amoCommand;
      this.ballot = ballot;
      this.status = status;
    }
  }

  @Data
  public static class PValue implements Serializable {
    private Ballot ballot;
    private int slotNum;
    private AMOCommand amoCommand;

    public PValue(Ballot ballot, int slotNum, AMOCommand amoCommand) {
      this.ballot = ballot;
      this.slotNum = slotNum;
      this.amoCommand = amoCommand;
    }
  }

  /** Paxos Data Structures: **/

  // isLeaderElected and ballotHighestSeen together define who this server thinks the current leader is
  private boolean isLeaderElected;
  private Ballot ballotHighestSeen;

  // replica uses this to determine when it should start leader election again
  private boolean gotHeartbeatFromLeader;

  // Used to bribe acceptors (P1) so that proposed values (P2) respect the write-once register
  // abstraction once a value is chosen. The ballots are totally ordered among all servers.
  private Ballot ballotSelf;

  // Combines replica and acceptor state on each log slot.
  //  - Replica State: Track which slots have been proposed (“accepted”), chosen, and empty.
  //                   This enables the replica to know how to handle client requests or decision messages.
  //  - Acceptor State: Track the value accepted for each slot with the highest ballot number.
  //                    This enables the acceptor (in conjunction with the replicas) to achieve
  //                    invariant A4/5 (once majority accepts, can’t overwrite).
  private final HashMap<Integer, LogEntry> logValues;
  private int slotOut; // the earliest non-executed slot

  // Replica (acting as a scout) uses this during leader election (P1) to wait for
  // a majority of acceptors to adopt their ballot
  private HashSet<Address> scoutWaitFor;

  // Leader (acting as commander) uses this to wait for a majority of acceptors to accept a
  // value for a slot (P2a sent, P2b received)
  private HashMap<Integer, HashSet<Address>> commanderWaitForPerSlot;

  // Garbage collection: track each server's execution progress for coordinated GC
  private HashMap<Address, Integer> serverSlotOuts; // follower's slotOut values
  private int globalMinSlotOut; // highest slot that all servers have executed (safe to GC)

  /* -----------------------------------------------------------------------------------------------
   *  Construction and Initialization
   * ---------------------------------------------------------------------------------------------*/
  public PaxosServer(Address address, Address[] servers, Application app) {
    super(address);
    this.servers = servers;

    this.amoApplication = new AMOApplication<>(app, new HashMap<>());

    /**
     * constructing PAXOS data structures:
     */
    this.ballotSelf = new Ballot(0, address);

    // no one is elected yet, perform leader election upon init()
    this.isLeaderElected = false;
    this.ballotHighestSeen = new Ballot(0, address);
    this.gotHeartbeatFromLeader = false;

    this.logValues = new HashMap<>();
    this.slotOut = LOG_START;

    this.scoutWaitFor = new HashSet<>();
    this.commanderWaitForPerSlot = new HashMap<>();

    this.serverSlotOuts = new HashMap<>();
    this.globalMinSlotOut = 0;
  }

  @Override
  public void init() {
    assertWithMessage(!this.isLeaderElected, "PaxosServer.init: no one should be elected yet");
    initLeaderElection();

    // set up pulsating heartbeat check timers to know when to re-initiate leader election
    set(new HeartbeatCheckTimer(), HeartbeatCheckTimer.HEARTBEAT_CHECK_RETRY_MILLIS);
    set(new HeartbeatTimer(), HeartbeatTimer.HEARTBEAT_RETRY_MILLIS);

    // set up pulsating repropose timer that the leader uses to drive consensus
    set(new ReproposeTimer(), ReproposeTimer.REPROPOSE_RETRY_MILLIS);
  }

  /* -----------------------------------------------------------------------------------------------
   *  Message Handlers - Replicas
   * ---------------------------------------------------------------------------------------------*/
  private void handlePaxosRequest(PaxosRequest m, Address sender) {
    // a server that has already executed the request can immediately send back a reply
    assertWithMessage(!isCmdNoOp(m.command()), "PaxosServer.handlePaxosRequest: client can never send noop");

    if (!this.isLeaderElected) {
      // still in leader election, drive progress
      assertWithMessage(!isMinority(this.scoutWaitFor), "PaxosServer.handlePaxosRequest: non-leader waiting for minority but still in leader election");
      sendAllExceptSelf(new P1a(this.ballotSelf));
    }

    if (this.amoApplication.alreadyExecuted(m.command())) {
      AMOResult amoResult = this.amoApplication.execute(m.command());
      send(new PaxosReply(amoResult), sender);
      return;
    }

    // replicas that are not the leader will not perform request processing
    if (!isLeader()) { return; }

    switch (getReqLogStatus(m)) {
      case EMPTY:
        // leader will put command into first empty slot, and send P2a message to all
        int emptySlotNum = findFirstEmptySlot();
        logValues.put(emptySlotNum, new LogEntry(m.command(), this.ballotSelf, PaxosLogSlotStatus.ACCEPTED));

        resetCommanderWaitFor(emptySlotNum);
        sendAllExceptSelf(new P2a(new PValue(this.ballotSelf, emptySlotNum, m.command())));
        break;
      case ACCEPTED:
        // leader will repropose request to drive progress
        // NOTE: the CommanderWaitFor does not need to be reset
        int reqLogSlot = getReqLogSlot(m);
        LogEntry entry = this.logValues.get(reqLogSlot);

        assertWithMessage(this.commanderWaitForPerSlot.containsKey(reqLogSlot),
                        "PaxosServer: leader should have already set commanderWaitFor for each accepted entry");
        assertWithMessage(this.ballotSelf.equals(entry.ballot()),
                        "PaxosServer: leader should already have accepted all accepted log entries");

        reproposeAllAcceptedSlots();
        break;
      case CHOSEN:
        // chosen but not executed => gaps
        fillGapsWithNoop(getReqLogSlot(m));
        reproposeAllAcceptedSlots();
        break;
      case CLEARED:
        // assertWithMessage(false,
        //     "PaxosServer: cleared command " + m.command() + " must already be executed");
        break;
    }
  }

  private void handleP1b(P1b p1b, Address sender) {
    // only process P1b during leader election
    if (this.isLeaderElected) { return; }

    // at this point, replica is still performing leader election
    assertWithMessage(this.ballotSelf.equals(this.ballotHighestSeen),
                    "PaxosServer.p1b: replica in leader election should still think it has the highest ballot");

    if (p1b.ballot().compareTo(this.ballotSelf) < 0) {
      // ignore => probably from previous phase of leader election
    } else if (p1b.ballot().compareTo(this.ballotSelf) == 0) {
      // iteratively merge logs
      mergeLog(p1b.log());
      this.scoutWaitFor.remove(sender);

      if (isMinority(this.scoutWaitFor)) {
        // LEADER ELECTED!!!
        this.isLeaderElected = true;
        this.commanderWaitForPerSlot = new HashMap<>();
        assertWithMessage(isLeader(), "PaxosServer.handleP1b: finished leader election but not leader");
        cleanupLeaderLog();
        reproposeAllAcceptedSlots();
      }
    } else {
      assertWithMessage(false, "PaxosServer.p1b: acceptors should not currently send failures");
    }
  }

  private void handleDecision(Decision decision, Address sender) {
    PValue pValDecision = decision.pValue();

    assertWithMessage(!pValDecision.ballot().equals(this.ballotSelf),
                    "PaxosServer.handleDecision: server " + this.address() + " sent itself decision");

    // to reduce concurrent leader time, adopt during decision
    if (pValDecision.ballot().compareTo(this.ballotHighestSeen) > 0) {
      changeBallotOnPreemption(pValDecision.ballot());
    }

    switch (status(pValDecision.slotNum())) {
      case EMPTY:
        setChosenAndExecPrefix(pValDecision);
        break;
      case ACCEPTED:
        LogEntry entryInternal = this.logValues.get(pValDecision.slotNum());
        assertWithMessage(entryInternal.ballot().compareTo(pValDecision.ballot()) <= 0 || entryInternal.amoCommand().equals(pValDecision.amoCommand()),
                          "PaxosServer.handleDecision: ballot being proposed in our log has higher ballot and different command");
        setChosenAndExecPrefix(pValDecision);
        break;
      case CHOSEN:
        // verify commands match (unless slot has been garbage collected)
        if (this.logValues.containsKey(pValDecision.slotNum())) {
          assertWithMessage(pValDecision.amoCommand().equals(this.logValues.get(pValDecision.slotNum()).amoCommand()),
                            "PaxosServer.handleDecision: two different commands chosen for same slot");
        }
        // do not need to do anything else
        break;
      case CLEARED:
        // assertWithMessage(false, "PaxosServer.handleDecision: handle CLEARED case");

        // slot already garbage collected, we've executed it already, ignore
        break;
    }

    if (isCmdNoOp(pValDecision.amoCommand())) {
      // do not need to do anything
    }
    else if (this.amoApplication.alreadyExecuted(pValDecision.amoCommand())) {
      // can send response back to client
      AMOResult amoResult = this.amoApplication.execute(pValDecision.amoCommand());
      send(new PaxosReply(amoResult), pValDecision.amoCommand().address());
    }
    else {
      // DO NOT DO ANYTHING HERE: non-leaders can receive decisions
    }
  }

  private void handleHeartbeat(Heartbeat heartbeat, Address sender) {
    assertWithMessage(!heartbeat.ballot().address().equals(this.address()), "PaxosServer.handleHeartbeat: Got heartbeat from self");

    if (heartbeat.ballot().compareTo(this.ballotHighestSeen) > 0) {
      // elect the sender of the heartbeat as the new leader (also sets gotHeartbeat)
      changeBallotOnPreemption(heartbeat.ballot());
    }
    else if (isAnotherServerElected() && heartbeat.ballot().equals(this.ballotHighestSeen)) {
      this.gotHeartbeatFromLeader = true;
    }
    mergeLog(heartbeat.log());

    // garbage collection: update our globalMinSlotOut from leader, then clear old slots
    if (heartbeat.globalMinSlotOut() > 0) {
      this.globalMinSlotOut = heartbeat.globalMinSlotOut();
      clearSlotsUpTo(this.globalMinSlotOut);
    }

    // send back our execution progress to the leader
    send(new HeartbeatReply(this.slotOut), sender);
  }

  private void handleHeartbeatReply(HeartbeatReply reply, Address sender) {
    if (!isLeader()) { return; }

    // update the sender's execution progress
    this.serverSlotOuts.put(sender, reply.slotOut());

    // if sender is behind, send them missing decisions to help them catch up
    // start from their slotOut, but don't send already garbage collected slots
    int startSlot = Math.max(reply.slotOut(), this.globalMinSlotOut + 1);
    for (int slot = startSlot; slot < this.slotOut; slot++) {
      if (status(slot) == PaxosLogSlotStatus.CHOSEN && this.logValues.containsKey(slot)) {
        LogEntry entry = this.logValues.get(slot);
        send(new Decision(new PValue(entry.ballot(), slot, entry.amoCommand())), sender);
      }
    }

    // calculate the global minimum slotOut (include leader's own slotOut)
    int minSlotOut = this.slotOut;
    for (Integer followerSlotOut : this.serverSlotOuts.values()) {
      minSlotOut = Math.min(minSlotOut, followerSlotOut);
    }

    this.globalMinSlotOut = minSlotOut - 1;
  }

  /* -----------------------------------------------------------------------------------------------
   *  Message Handlers - Commanders
   * ---------------------------------------------------------------------------------------------*/

  private void handleP2b(P2b p2b, Address sender) {
    PValue p2bPVal = p2b.pValue();
    
    // only a leader should process p2b messages
    if (!isLeader()) { return; }

    // it can be assumed that p2b is only received when the acceptor
    // has adopted a ballot with the same address as this server
    assertWithMessage(p2bPVal.ballot().address().equals(this.address()) && p2bPVal.ballot().compareTo(this.ballotSelf) <= 0,
                    "PaxosServer.handleP2b: receiving failed p2b reply (which acceptors currently do not send)");

    // do not process smaller ballots (stale)
    if (p2bPVal.ballot().compareTo(this.ballotSelf) < 0) { return; }

    switch (status(p2bPVal.slotNum())) {
      case EMPTY:
        assertWithMessage(false, "PaxosServer.handleP2b: slot " + p2bPVal.slotNum() + " is empty (even though this commander sent it)");
        break;
      case ACCEPTED:
        assertWithMessage(commanderWaitForPerSlot.containsKey(p2bPVal.slotNum()),
            "PaxosServer.handleP2b: server should still have accepted slot in cmdrWaitForPerSlot");

        // remove sender from commanderWaitFor for the slot they have accepted the proposal in
        commanderWaitForPerSlot.get(p2bPVal.slotNum()).remove(sender);
        if (isMinority(commanderWaitForPerSlot.get(p2bPVal.slotNum()))) {
          setChosenAndExecPrefix(p2bPVal);
          sendAllExceptSelf(new Decision(p2bPVal));
        }
        break;
      case CHOSEN:
        assertWithMessage(!commanderWaitForPerSlot.containsKey(p2bPVal.slotNum()),
            "PaxosServer.handleP2b: server should have removed chosen slot from cmdrWaitForPerSlot");
        // can just ignore p2b
        break;
      case CLEARED:
        // assertWithMessage(false, "PaxosServer.handleP2b: handle cleared case");
        // slot already garbage collected and executed, ignore p2b
        break;
    }
  }

  /* -----------------------------------------------------------------------------------------------
   *  Message Handlers - Acceptors
   * ---------------------------------------------------------------------------------------------*/

  private void handleP1a(P1a p1a, Address sender) {
    assertWithMessage(!p1a.ballot().address().equals(this.address()), "PaxosServer.handleP1a: should never get P1a from self");

    if (p1a.ballot().compareTo(this.ballotHighestSeen) > 0) {
      changeBallotOnPreemption(p1a.ballot());
      assertWithMessage(p1a.ballot().equals(this.ballotHighestSeen), "PaxosServer.handleP1a: should have adopted higher ballot by now");
      send(new P1b(this.ballotHighestSeen, this.logValues), sender);
    }
  }

  private void handleP2a(P2a p2a, Address sender) {
    PValue p2aPVal = p2a.pValue();

    assertWithMessage(p2aPVal.ballot().compareTo(this.ballotSelf) != 0,
                      "PaxosServer.handleP2a: server should never get their own P2a msg (for now)");

    // ignore P2a if the ballot in the request is lower than our highest seen (bribed by someone else)
    if (p2aPVal.ballot().compareTo(this.ballotHighestSeen) < 0) { return; }

    // adopt ballot if higher than currently highest seen (may change leader at this point)
    if (p2aPVal.ballot().compareTo(this.ballotHighestSeen) > 0) {
      changeBallotOnPreemption(p2aPVal.ballot());
    }

    // at this point, the ballot in the P2a request must match our highest seen
    assertWithMessage(this.ballotHighestSeen.equals(p2aPVal.ballot()),
                  "PaxosServer.handleP2a: highest ballot is " + this.ballotHighestSeen + " instead of " + p2aPVal.ballot());

    // case on the logEntry status of the slot number in our log
    switch (status(p2aPVal.slotNum())) {
      case EMPTY:
        logValues.put(p2aPVal.slotNum(), new LogEntry(p2aPVal.amoCommand(), p2aPVal.ballot(), PaxosLogSlotStatus.ACCEPTED));
        break;
      case ACCEPTED:
        assertWithMessage(this.ballotHighestSeen.compareTo(logValues.get(p2aPVal.slotNum()).ballot()) >= 0,
                          "PaxosServer.handleP2a: highest ballot is not at least as large as the ballot in any non-empty slot");
        logValues.put(p2aPVal.slotNum(), new LogEntry(p2aPVal.amoCommand(), p2aPVal.ballot(), PaxosLogSlotStatus.ACCEPTED));
        break;
      case CHOSEN:
        // verify command matches what we have if entry still exists
        if (logValues.containsKey(p2aPVal.slotNum())) {
          LogEntry existingEntry = logValues.get(p2aPVal.slotNum());
          // only check if we still have the entry (might be mid-GC)
          if (existingEntry != null && existingEntry.status() == PaxosLogSlotStatus.CHOSEN) {
            assertWithMessage(p2aPVal.amoCommand().equals(existingEntry.amoCommand()),
                          "PaxosServer.handleP2a: Cmd from request is not the same as cmd in slot");
          }
        }
        // do not need to do anything really
        break;
      case CLEARED:
        // assertWithMessage(false, "PaxosServer.handleP2a: cleared case unimplemented");
        // slot already garbage collected, we've executed it, just ack back
        break;
    }

    send(new P2b(p2aPVal), sender);
  }

  /* -----------------------------------------------------------------------------------------------
   *  Timer Handlers
   * ---------------------------------------------------------------------------------------------*/

  // pulsating timer that this server uses to check if the leader is alive,
  // and if not, will re-initiate leader election
  private void onHeartbeatCheckTimer(HeartbeatCheckTimer t) {
    if (isAnotherServerElected() && !this.gotHeartbeatFromLeader) {
      assertWithMessage(this.ballotHighestSeen.compareTo(this.ballotSelf) > 0,
                        "PaxosServer.onHeartbeatCheckTimer: this server expected heartbeat from another, but does not think another is the leader somehow");
      initLeaderElection();
    }

    this.gotHeartbeatFromLeader = false;
    set(t, HeartbeatCheckTimer.HEARTBEAT_CHECK_RETRY_MILLIS);
  }

  // pulsating timer that a leader uses to tell everyone they are alive
  private void onHeartbeatTimer(HeartbeatTimer t) {
    if (isLeader()) {
      // leader garbage collects only if all followers have reported in
      if (this.globalMinSlotOut > 0 && this.serverSlotOuts.size() == this.servers.length - 1) {
        clearSlotsUpTo(this.globalMinSlotOut);
      }
      sendAllExceptSelf(new Heartbeat(this.ballotSelf, this.logValues, this.globalMinSlotOut));
    }
    set(t, HeartbeatTimer.HEARTBEAT_RETRY_MILLIS);
  }

  private void onReproposeTimer(ReproposeTimer t) {
    if (isLeader()) {
      reproposeAllAcceptedSlots();
    }
    set(t, ReproposeTimer.REPROPOSE_RETRY_MILLIS);
  }

  /* -----------------------------------------------------------------------------------------------
   *  Utils
   * ---------------------------------------------------------------------------------------------*/

  // leader election helpers:

  // initializes the leader election phase by changing this server's state to
  // no longer think there's a leader, and to send out P1a messages to bribe acceptors.
  // This function assumes that the server has not gotten a heartbeat from a leader
  // within a given time interval.
  private void initLeaderElection() {
    assertWithMessage(!this.gotHeartbeatFromLeader, "PaxosServer.initLeaderElection: got heartbeat but still doing leader election");
    assertWithMessage(this.ballotHighestSeen != null, "PaxosServer.initLeaderElection: highest ballot uninitialized");
    assertWithMessage(!isLeader(), "PaxosServer.initLeaderElection: server thinks its the leader but is performing leader election");
    assertWithMessage(this.ballotHighestSeen.compareTo(this.ballotSelf) >= 0, "PaxosServer.initLeaderElection: ballot of dead leader should be at least as large as this server's ballot");

    this.isLeaderElected = false;
    this.ballotSelf = new Ballot(this.ballotHighestSeen.sequenceNum() + 1, this.address());

    // replica immediately adopts their own ballot (careful about reference vs copy!)
    this.ballotHighestSeen = new Ballot(this.ballotSelf.sequenceNum(), this.ballotSelf.address());

    // reset scoutWaitFor to FullSet \ {self} (this server already adopted
    // their own ballot) and send P1a to all except self to drive leader election
    this.scoutWaitFor = new HashSet<>();
    resetWaitFor(this.scoutWaitFor);
    sendAllExceptSelf(new P1a(this.ballotSelf));
  }

  // log helpers:

  // Finds the first empty slot in the log, and returns that integer.
  // If the log is empty, will return 1.
  private int findFirstEmptySlot() {
    // TODO: add in garbage collection logic
    int logSlotNum = LOG_START;
    while (logValues.containsKey(logSlotNum)) {
      logSlotNum++;
    }
    return logSlotNum;
  }

  // Get the log entry of the slot that holds the amoCommand in the request, and returns
  // NULL if no log slot contains the request's command.
  // It is assumed that at most one log slot will contain the command in the request.
  private int getReqLogSlot(PaxosRequest request) {
    assertWithMessage(!isCmdNoOp(request.command()), "PaxosServer.getReqLogSlot: client request is NOOP, when it should never be");

    for (Integer slotNum : this.logValues.keySet()) {
      LogEntry entry = this.logValues.get(slotNum);
      assertWithMessage(slotNum >= LOG_START, "PaxosServer.getReqLogSlot: logValues contains invalid slot " + slotNum);

      if (request.command().equals(entry.amoCommand())) {
        assertWithMessage(entry.status == PaxosLogSlotStatus.ACCEPTED || entry.status == PaxosLogSlotStatus.CHOSEN,
            "PaxosServer.getReqLogSlot: request in log has malformed status " + entry.status);
        return slotNum;
      }
    }
    return LOG_UNKNOWN;
  }

  // Get the log status of the slot that holds the amoCommand in the request.
  // It is assumed that at most one log slot will contain the command in the request.
  private PaxosLogSlotStatus getReqLogStatus(PaxosRequest request) {
    // TODO: handle returning CLEARED status
    int reqLogSlot = getReqLogSlot(request);
    return (reqLogSlot == LOG_UNKNOWN) ? PaxosLogSlotStatus.EMPTY : status(reqLogSlot);
  }

  // Will set the slot associated with the `pValue` to CHOSEN, and will then
  // execute the largest prefix of CHOSEN commands in the log. It is assumed
  // that the slot being set to CHOSEN has not been cleared nor executed before.
  private void setChosenAndExecPrefix(PValue pValue) {
    assertWithMessage(status(pValue.slotNum()) != PaxosLogSlotStatus.CLEARED,
                      "PaxosServer.setChosenAndExecPrefix: slot " + pValue.slotNum() + " is cleared but being set to chosen");

    this.logValues.put(pValue.slotNum(),
        new LogEntry(pValue.amoCommand(), pValue.ballot(), PaxosLogSlotStatus.CHOSEN)
    );
    this.commanderWaitForPerSlot.remove(pValue.slotNum()); // this server is no longer waiting for the slot to be decided

    while (status(this.slotOut) == PaxosLogSlotStatus.CHOSEN) {
      AMOCommand amoCommandSlotOut = this.logValues.get(this.slotOut).amoCommand();

      if (!isCmdNoOp(amoCommandSlotOut)) {
        // IMPORTANT POINT: the same command may be chosen for multiple slots, so it is not the case that things after slotOut are not already executed
        // assertWithMessage(!this.amoApplication.alreadyExecuted(amoCommandSlotOut),
        //     "PaxosServer.setChosenAndExecPrefix (server " + this.address() + "): slot " + this.slotOut + " is CHOSEN, but was alreadyExecuted");
        this.amoApplication.execute(amoCommandSlotOut);
      }
      this.slotOut += 1;
    }
  }

  // fills all EMPTY slots before the argument to ACCEPTED with a NOOP command, and resets
  // the WaitFor for each slot (so that the leader can begin sending P2a messages right away).
  // this function assumes that the server calling this function is the leader (as only
  // the leader can arbitrarily fill empty slots with accepted commands), and
  // that the slot for the argument is CHOSEN
  private void fillGapsWithNoop(int slotEndFilling) {
    assertWithMessage(isLeader(), "PaxosServer.fillGapsWithNoop: non-leader trying to fill gaps");
    assertWithMessage(status(slotEndFilling) == PaxosLogSlotStatus.CHOSEN, "PaxosServer.fillGapsWithNoop: end of gap is not a CHOSEN command");
    assertWithMessage(slotEndFilling >= LOG_START, "PaxosServer.fillGapsWithNoop: improper log slot " + slotEndFilling);

    for (int slotToFill = LOG_START; slotToFill < slotEndFilling; slotToFill++) {
      if (status(slotToFill) == PaxosLogSlotStatus.EMPTY) {
        this.logValues.put(slotToFill,
            new LogEntry(genCmdNoOp(), this.ballotSelf, PaxosLogSlotStatus.ACCEPTED)
        );
        resetCommanderWaitFor(slotToFill);
      }
    }
  }

  // send a P2a message for each accepted slot to all except self.
  // this function assumes that the caller is the leader, that
  // every slot has a commanderWaitFor entry (should already be populated)
  // and that every accepted slot in the leader's log has the leader's ballot
  // (allowing for the leader to skip sending to themselves)
  private void reproposeAllAcceptedSlots() {
    assertWithMessage(isLeader(), "PaxosServer.reproposeAllAcceptedSlots: caller is not leader");

    for (Integer slotNum : this.logValues.keySet()) {
      if (status(slotNum) == PaxosLogSlotStatus.ACCEPTED) {
        assertWithMessage(this.commanderWaitForPerSlot.containsKey(slotNum),
                        "PaxosServer.reproposeAllAcceptedSlots: slot " + slotNum + " is accepted but no cmdWaitFor set");
        assertWithMessage(!isMinority(this.commanderWaitForPerSlot.get(slotNum)),
                        "PaxosServer.reproposeAllAcceptedSlots: slot " + slotNum + " already has minority cmdWaitFor");
        assertWithMessage(this.logValues.get(slotNum).ballot().equals(this.ballotSelf),
                        "PaxosServer.reproposeAllAcceptedSlots: leader should have already replaced all accepted ballots with their own");

        LogEntry entry = this.logValues.get(slotNum);
        sendAllExceptSelf(new P2a(
            new PValue(entry.ballot(), slotNum, entry.amoCommand()))
        );
      }
    }
  }

  // compares the two log entries given as argument, and returns the log entry with "higher importance"
  // as described in the design doc. It is assumed at least the external entry is Non-Null
  //  - ACCEPTED > EMPTY
  //  - CHOSEN > ACCEPTED
  //  - If both are ACCEPTED, return the one containing the higher ballot
  //  - If both are CHOSEN, assert that both commands in the entries are the same, and return either one
  private LogEntry compareLogEntries(LogEntry entryInternal, @NonNull LogEntry entryExternal) {
    if (entryInternal == null) {
      return entryExternal;
    }
    assertWithMessage(entryExternal.status() == PaxosLogSlotStatus.ACCEPTED || entryExternal.status() == PaxosLogSlotStatus.CHOSEN,
                      "PaxosServer.mergeLog: external log entry has bad status " + entryExternal.status());
    assertWithMessage(entryInternal.status() == PaxosLogSlotStatus.ACCEPTED || entryInternal.status() == PaxosLogSlotStatus.CHOSEN,
                      "PaxosServer.mergeLog: internal log entry has bad status " + entryInternal.status());

    if (entryInternal.status() == PaxosLogSlotStatus.CHOSEN) {
      assertWithMessage(entryExternal.status() != PaxosLogSlotStatus.CHOSEN || entryInternal.amoCommand().equals(entryExternal.amoCommand()),
                        "PaxosServer.compareLogEntries: two different commands chosen for same slot");
      return entryInternal;
    } else if (entryExternal.status() == PaxosLogSlotStatus.CHOSEN) {
      assertWithMessage(entryInternal.ballot().compareTo(entryExternal.ballot()) <= 0 || entryInternal.amoCommand().equals(entryExternal.amoCommand()),
                        "PaxosServer.compareLogEntries: higher proposed value for chosen slot has different command");
      return entryExternal;
    } else {
      // if both are accepted for the same slot, return the entry with the higher ballot
      return (entryInternal.ballot().compareTo(entryExternal.ballot()) >= 0) ? entryInternal : entryExternal;
    }
  }

  // will merge the chosen and accepted entries from the argument's log into
  // this server's local log. This function can be called at any time from any type
  // of server, given that the argument is a log constructed from a faithful execution
  // of this protocol. If the leader calls this function, this function will refactor
  // its log to no longer wait for chosen slots, and begin waiting for newly accepted slots
  private void mergeLog(HashMap<Integer, LogEntry> logExternal) {
    for (Integer slotNumExternal : logExternal.keySet()) {
      // skip slots that have already been garbage collected
      if (status(slotNumExternal) == PaxosLogSlotStatus.CLEARED) {
        continue;
      }

      LogEntry entryInternal = this.logValues.getOrDefault(slotNumExternal, null);
      LogEntry entryExternal = logExternal.get(slotNumExternal);

      LogEntry entryLatestForSlot =  compareLogEntries(entryInternal, entryExternal);

      if (entryLatestForSlot.status() == PaxosLogSlotStatus.CHOSEN) {
        setChosenAndExecPrefix(new PValue(entryLatestForSlot.ballot(), slotNumExternal, entryLatestForSlot.amoCommand()));
      } else {
        // just a regular accepted slot
        this.logValues.put(slotNumExternal, entryLatestForSlot);
      }
    }
    cleanupLeaderLog();
  }

  // clean up the leader's log by removing lingering commanderWaitFor for CHOSEN entries,
  // ensuring ACCEPTED entries have commanderWaitFor, and that the ballot inside ACCEPTED
  // entries are set to the leader's ballot (leader must accept all accepted slots first)
  private void cleanupLeaderLog() {
    if (!isLeader()) { return; }

    // remove any entries that have been garbage collected
    this.logValues.entrySet().removeIf(entry -> entry.getKey() <= this.globalMinSlotOut);

    this.logValues.replaceAll((slotNum, entry) -> {
      switch (status(slotNum)) {
        case ACCEPTED:
          if (!this.commanderWaitForPerSlot.containsKey(slotNum)) {
            resetCommanderWaitFor(slotNum);
          }
          return new LogEntry(entry.amoCommand(), this.ballotSelf, entry.status());
        case CHOSEN:
          this.commanderWaitForPerSlot.remove(slotNum);
          return entry;
        default:
          assertWithMessage(false, "PaxosServer.cleanupLeaderLog: empty or cleared entry in log");
          return entry;
      }
    });
    reproposeAllAcceptedSlots();
  }

  // clears all log slots up to and including the given slot number
  private void clearSlotsUpTo(int slotNum) {
    // remove all entries <= slotNum from the log
    this.logValues.entrySet().removeIf(entry -> entry.getKey() <= slotNum);
  }

  // leader and ballot stuff:

  // Returns whether this server is the leader, i.e. thinks a
  // leader is elected and is part of the highest ballot seen
  private boolean isLeader() {
    return this.isLeaderElected && this.ballotHighestSeen.address.equals(this.address());
  }
  // Returns whether this server thinks another server is elected.
  private boolean isAnotherServerElected() {
    return this.isLeaderElected && !this.ballotHighestSeen.address.equals(this.address());
  }

  // Will update the highest ballot seen to the argument, and set the
  // leader to the server associated with the highest ballot
  private void changeBallotOnPreemption(Ballot ballot) {
    assertWithMessage(ballot.compareTo(this.ballotHighestSeen) > 0,
                      "PaxosServer.changeBallotOnPreemption: ballot passed in not higher than ours");

    this.ballotHighestSeen = ballot;
    this.isLeaderElected = true;
    this.gotHeartbeatFromLeader = true;
  }

  // WaitFor Helpers:

  // reset a WaitFor set in-place to the FullSet \ {self}, implying
  // this server will wait for everybody except itself. This function
  // assumes the set passed in is initialized as the empty set.
  private void resetWaitFor(@NonNull HashSet<Address> waitFor) {
    assertWithMessage(waitFor.isEmpty(), "PaxosServer.resetWaitFor: waitFor set is not initialized as the empty set");
    for (Address server : servers) {
      if (!server.equals(this.address())) {
        waitFor.add(server);
      }
    }
  }

  // reset CommanderWaitFor for a slot to the FullSet \ { self }
  private void resetCommanderWaitFor(int slotNum) {
    this.commanderWaitForPerSlot.put(slotNum, new HashSet<>());
    resetWaitFor(this.commanderWaitForPerSlot.get(slotNum));
  }

  // helpers for NOOP commands:
  private AMOCommand genCmdNoOp() {
    return new AMOCommand(null, null, -1);
  }

  private boolean isCmdNoOp(AMOCommand amoCommand) {
    return amoCommand.command() == null && amoCommand.address() == null && amoCommand.sequenceNum() == -1;
  }

  // message helpers:

  private void sendAllExceptSelf(Message m) {
    for (Address server : servers) {
      if (!server.equals(this.address())) {
        send(m, server);
      }
    }
  }

  // basic helpers:

  private boolean isMinority(HashSet<Address> serverSet) {
    return serverSet.size() < Math.ceil(this.servers.length / 2.0);
  }

  private void assertWithMessage(boolean b, String m) {
    if (!b) {
      System.out.println(m);
      System.exit(1);
    }
  }

  /* -----------------------------------------------------------------------------------------------
   *  Interface Methods
   *
   *  Be sure to implement the following methods correctly. The test code uses them to check
   *  correctness more efficiently.
   * ---------------------------------------------------------------------------------------------*/

  /**
   * Return the status of a given slot in the server's local log.
   *
   * <p>If this server has garbage-collected this slot, it should return {@link
   * PaxosLogSlotStatus#CLEARED} even if it has previously accepted or chosen command for this slot.
   * If this server has both accepted and chosen a command for this slot, it should return {@link
   * PaxosLogSlotStatus#CHOSEN}.
   *
   * <p>Log slots are numbered starting with 1.
   *
   * @param logSlotNum the index of the log slot
   * @return the slot's status
   * @see PaxosLogSlotStatus
   */
  public PaxosLogSlotStatus status(int logSlotNum) {
    if (logSlotNum <= this.globalMinSlotOut) {
      return PaxosLogSlotStatus.CLEARED;
    }

    if (this.logValues.containsKey(logSlotNum)) {
      PaxosLogSlotStatus slotStatus = logValues.get(logSlotNum).status();
      assertWithMessage(slotStatus == PaxosLogSlotStatus.ACCEPTED || slotStatus == PaxosLogSlotStatus.CHOSEN,
                                      "PaxosServer.chosen: slot " + logSlotNum + " has status " + slotStatus);
      return slotStatus;
    }
    return PaxosLogSlotStatus.EMPTY;
  }

  /**
   * Return the command associated with a given slot in the server's local log.
   *
   * <p>If the slot has status {@link PaxosLogSlotStatus#CLEARED} or {@link
   * PaxosLogSlotStatus#EMPTY}, this method should return {@code null}. Otherwise, return the
   * command this server has chosen or accepted, according to {@link PaxosServer#status}.
   *
   * <p>If clients wrapped commands in {@link framework.atmostonce.AMOCommand}, this method should
   * unwrap them before returning.
   *
   * <p>Log slots are numbered starting with 1.
   *
   * @param logSlotNum the index of the log slot
   * @return the slot's contents or {@code null}
   * @see PaxosLogSlotStatus
   */
  public Command command(int logSlotNum) {
    switch (status(logSlotNum)) {
      case EMPTY:
        break;
      case ACCEPTED: case CHOSEN:
        return this.logValues.get(logSlotNum).amoCommand().command();
      case CLEARED:
        // assertWithMessage(false, "PaxosServer.command: handle cleared case on slot " + logSlotNum);
        // cleared slots have been garbage collected, return null
        break;
    }
    return null;
  }

  /**
   * Return the index of the first non-cleared slot in the server's local log. The first non-cleared
   * slot is the first slot which has not yet been garbage-collected. By default, the first
   * non-cleared slot is 1.
   *
   * <p>Log slots are numbered starting with 1.
   *
   * @return the index in the log
   * @see PaxosLogSlotStatus
   */
  public int firstNonCleared() {
    return this.globalMinSlotOut + 1;
  }

  /**
   * Return the index of the last non-empty slot in the server's local log, according to the defined
   * states in {@link PaxosLogSlotStatus}. If there are no non-empty slots in the log, this method
   * should return 0.
   *
   * <p>Log slots are numbered starting with 1.
   *
   * @return the index in the log
   * @see PaxosLogSlotStatus
   */
  public int lastNonEmpty() {
    int slot_nonempty_max = 0;
    for (Integer slot : this.logValues.keySet()) {
      // only consider slots that haven't been cleared
      if (slot > this.globalMinSlotOut) {
        assertWithMessage(status(slot) == PaxosLogSlotStatus.ACCEPTED || status(slot) == PaxosLogSlotStatus.CHOSEN,
                          "PaxosServer.lastNonEmpty: slot " + slot + " in log has status " + status(slot));
        slot_nonempty_max = Math.max(slot_nonempty_max, slot);
      }
    }
    return slot_nonempty_max;
  }
}
