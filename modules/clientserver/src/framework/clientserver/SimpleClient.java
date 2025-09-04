package framework.clientserver;

import framework.atmostonce.AMOCommand;
import framework.atmostonce.AMOResult;
import framework.Address;
import framework.Client;
import framework.Command;
import framework.Node;
import framework.Result;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * Simple client that sends requests to a single server and returns responses.
 *
 * <p>See the documentation of {@link Client} and {@link Node} for important implementation notes.
 */
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
class SimpleClient extends Node implements Client {
  private final Address serverAddress;
  private int sequenceNum; // for determining if the reply matches the ongoing request
  private Request request;
  private Result result; // for notifying

  /* -----------------------------------------------------------------------------------------------
   *  Construction and Initialization
   * ---------------------------------------------------------------------------------------------*/
  public SimpleClient(Address address, Address serverAddress) {
    super(address);
    this.serverAddress = serverAddress;
    this.sequenceNum = 0;
  }

  @Override
  public synchronized void init() {
    // No initialization necessary
  }

  /* -----------------------------------------------------------------------------------------------
   *  Client Methods
   * ---------------------------------------------------------------------------------------------*/
  @Override
  public synchronized void sendCommand(Command command) {
    this.request = new Request(new AMOCommand(command, this.sequenceNum));
    this.result = null;

    send(this.request, this.serverAddress);
    set(new ClientTimer(this.request), ClientTimer.CLIENT_RETRY_MILLIS);
  }

  @Override
  public synchronized boolean hasResult() {
    return this.result != null;
  }

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
  private synchronized void handleReply(Reply m, Address sender) {
    AMOResult amoResult = m.result();

    if (amoResult.wasSuccessfullyExecuted() && amoResult.sequenceNum() == this.sequenceNum) {
      // ongoing request was successfully executed, client can move on by incrementing
      // sequence number (in effect to match amoApp's sequence number/"time").
      this.result = amoResult.result();
      this.sequenceNum++;
      notify();
    }

    if (!amoResult.wasSuccessfullyExecuted() && amoResult.sequenceNum() > this.sequenceNum) {
      // "time" at the client is out of sync with server,
      // update sequenceNum, send new request with new sequence number, and set new timer
      Command command = this.request.command().command();
      AMOCommand amoCommandUpdatedSeqNum = new AMOCommand(command, amoResult.sequenceNum());

      this.request = new Request(amoCommandUpdatedSeqNum);
      this.sequenceNum = amoResult.sequenceNum();

      send(this.request, this.serverAddress);
      set(new ClientTimer(this.request), ClientTimer.CLIENT_RETRY_MILLIS);
    }
  }

  /* -----------------------------------------------------------------------------------------------
   *  Timer Handlers
   * ---------------------------------------------------------------------------------------------*/
  private synchronized void onClientTimer(ClientTimer t) {
    // timer is not stale if sequence number matches client's sequence number.
    // client's current sequence number is an identifier for an ongoing request.
    if (t.request().command().sequenceNum() == this.sequenceNum) {
      send(t.request(), this.serverAddress);
      set(t, ClientTimer.CLIENT_RETRY_MILLIS);
    }
  }
}
