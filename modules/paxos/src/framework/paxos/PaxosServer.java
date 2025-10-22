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
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import lombok.extern.java.Log;

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

  /** Common Data Structures Used By Most Roles: */

  // isLeaderElected and ballotHighestSeen together define who this server thinks the current leader is
  private boolean isLeaderElected;
  private Ballot ballotHighestSeen;

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

  // Leader (acting as commander) uses this to wait for a majority of acceptors to accept a
  // value for a slot (P2a sent, P2b received)
  private HashMap<Integer, HashSet<Address>> commanderWaitForPerSlot;

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

    // every server initially thinks that server[0] is the leader (no split brain)
    this.isLeaderElected = true; // TODO: change to false (but for now skip leader election process)
    this.ballotHighestSeen = new Ballot(0, servers[0]);

    this.logValues = new HashMap<>();
    this.slotOut = LOG_START;

    this.commanderWaitForPerSlot = new HashMap<>();
  }

  @Override
  public void init() {
    // Your code here...
  }

  /* -----------------------------------------------------------------------------------------------
   *  Message Handlers - Replicas
   * ---------------------------------------------------------------------------------------------*/
  private void handlePaxosRequest(PaxosRequest m, Address sender) {
    // a server that has already executed the request can immediately send back a reply
    assertWithMessage(!isCmdNoOp(m.command()), "PaxosServer.handlePaxosRequest: client can never send noop");

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
                        "PaxosServer: leader should already have accept all accepted log entries");

        // TODO: maybe change this to repropose to all
        sendAllExceptSelf(new P2a(new PValue(entry.ballot(), reqLogSlot, entry.amoCommand())));
        break;
      case CHOSEN:
        fillGapsWithNoop(getReqLogSlot(m));
        reproposeAllAcceptedSlots();
        break;
      case CLEARED:
        assertWithMessage(false,
            "PaxosServer: cleared command " + m.command() + " must already be executed");
        break;
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
        assertWithMessage(pValDecision.ballot().compareTo(this.logValues.get(pValDecision.slotNum()).ballot()) >= 0,
                          "PaxosServer.handleDecision: ballot in decision is not larger than ballot in log slot for decision");
        setChosenAndExecPrefix(pValDecision);
        break;
      case CHOSEN:
        assertWithMessage(pValDecision.amoCommand().equals(this.logValues.get(pValDecision.slotNum()).amoCommand()),
                          "PaxosServer.handleDecision: two different commands chosen for same slot");
        // do not need to do anything else
        break;
      case CLEARED:
        assertWithMessage(false, "PaxosServer.handleDecision: handle CLEARED case");
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
          commanderWaitForPerSlot.remove(p2bPVal.slotNum());
          sendAllExceptSelf(new Decision(p2bPVal));
        }
        break;
      case CHOSEN:
        assertWithMessage(!commanderWaitForPerSlot.containsKey(p2bPVal.slotNum()),
            "PaxosServer.handleP2b: server should have removed chosen slot from cmdrWaitForPerSlot");
        // can just ignore p2b
        break;
      case CLEARED:
        assertWithMessage(false, "PaxosServer.handleP2b: handle cleared case");
        break;
    }
  }

  /* -----------------------------------------------------------------------------------------------
   *  Message Handlers - Acceptors
   * ---------------------------------------------------------------------------------------------*/

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
        assertWithMessage(p2aPVal.amoCommand().equals(logValues.get(p2aPVal.slotNum()).amoCommand()),
                      "PaxosServer.handleP2a: Cmd from request is not the same as cmd in slot");
        // do not need to do anything really
        break;
      case CLEARED:
        assertWithMessage(false, "PaxosServer.handleP2a: cleared case unimplemented");
        break;
    }

    send(new P2b(p2aPVal), sender);
  }

  /* -----------------------------------------------------------------------------------------------
   *  Timer Handlers
   * ---------------------------------------------------------------------------------------------*/
  // Your code here...

  /* -----------------------------------------------------------------------------------------------
   *  Utils
   * ---------------------------------------------------------------------------------------------*/

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

    while (status(this.slotOut) == PaxosLogSlotStatus.CHOSEN) {
      AMOCommand amoCommandSlotOut = this.logValues.get(this.slotOut).amoCommand();

      if (!isCmdNoOp(amoCommandSlotOut)) {
        assertWithMessage(!this.amoApplication.alreadyExecuted(amoCommandSlotOut),
            "PaxosServer.setChosenAndExecPrefix (server " + this.address() + "): slot " + this.slotOut + " is being set to CHOSEN, but was alreadyExecuted");
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

  // leader and ballot stuff:

  // Returns whether this server is the leader, i.e. thinks a
  // leader is elected and is part of the highest ballot seen
  private boolean isLeader() {
    return this.isLeaderElected && this.ballotHighestSeen.address.equals(this.address());
  }

  // Will update the highest ballot seen to the argument, and set the
  // leader to the server associated with the highest ballot
  private void changeBallotOnPreemption(Ballot ballot) {
    assertWithMessage(ballot.compareTo(this.ballotHighestSeen) > 0,
                      "PaxosServer.changeBallotOnPreemption: ballot passed in not higher than ours");

    this.ballotHighestSeen = ballot;
    this.isLeaderElected = true;
    // TODO: add heartbeat boolean change according to design
  }

  // CommanderWaitForPerSlotHelper:

  // reset CommanderWaitFor for a slot to the FullSet \ { self }
  private void resetCommanderWaitFor(int slotNum) {
    this.commanderWaitForPerSlot.put(slotNum, new HashSet<>());
    for (Address server : servers) {
      if (!server.equals(this.address())) {
        this.commanderWaitForPerSlot.get(slotNum).add(server);
      }
    }
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
    // TODO: eventually handle garbage-collection
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
        assertWithMessage(this.logValues.containsKey(logSlotNum),
                        "PaxosServer.command: slot " + logSlotNum + " is accepted but not in log");
        assertWithMessage(!isCmdNoOp(this.logValues.get(logSlotNum).amoCommand()), "PaxosServer.command: handle NOOP case");
        return this.logValues.get(logSlotNum).amoCommand().command();
      case CLEARED:
        assertWithMessage(false, "PaxosServer.command: handle cleared case on slot " + logSlotNum);
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
    // TODO: implement this eventually once adding in garbage collection
    return LOG_START;
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
    // TODO: implement correctly when handling cleared slots (start at the last cleared slot)
    int slot_nonempty_max = 0;
    for (Integer slot : this.logValues.keySet()) {
      assertWithMessage(status(slot) == PaxosLogSlotStatus.ACCEPTED || status(slot) == PaxosLogSlotStatus.CHOSEN,
                        "PaxosServer.lastNonEmpty: slot " + slot + " in log has status " + status(slot));
      slot_nonempty_max = Math.max(slot_nonempty_max, slot);
    }
    return slot_nonempty_max;
  }
}
