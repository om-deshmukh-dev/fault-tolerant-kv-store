package framework.paxos;

import framework.atmostonce.AMOCommand;
import framework.atmostonce.AMOResult;
import framework.Address;
import framework.Client;
import framework.Command;
import framework.Node;
import framework.Result;
import lombok.EqualsAndHashCode;
import lombok.ToString;

@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public final class PaxosClient extends Node implements Client {
  private final Address[] servers;
  private int sequenceNum; // for determining if the reply matches the ongoing request
  private Result result; // for getResult()

  // Your code here...

  /* -----------------------------------------------------------------------------------------------
   *  Construction and Initialization
   * ---------------------------------------------------------------------------------------------*/
  public PaxosClient(Address address, Address[] servers) {
    super(address);
    this.servers = servers;
    this.sequenceNum = 0;
  }

  @Override
  public synchronized void init() {
    // No need to initialize
  }

  /* -----------------------------------------------------------------------------------------------
   *  Client Methods
   * ---------------------------------------------------------------------------------------------*/
  @Override
  public synchronized void sendCommand(Command operation) {
    PaxosRequest request = new PaxosRequest(new AMOCommand(operation, this.address(), this.sequenceNum));
    this.result = null;

    sendToAll(request);
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
   * Message Handlers
   * ---------------------------------------------------------------------------------------------*/
  private synchronized void handlePaxosReply(PaxosReply m, Address sender) {
    AMOResult amoResult = m.result();

    if (amoResult.sequenceNum() == this.sequenceNum) {
      this.result = amoResult.result();
      this.sequenceNum++;
      notify();
    }
  }

  /* -----------------------------------------------------------------------------------------------
   *  Timer Handlers
   * ---------------------------------------------------------------------------------------------*/
  private synchronized void onClientTimer(ClientTimer t) {
    if (t.request().command().sequenceNum() == this.sequenceNum) {
      sendToAll(t.request());
      set(t, ClientTimer.CLIENT_RETRY_MILLIS);
    }
  }

  /**
   * Helper Functions:
   */

  private void sendToAll(PaxosRequest request) {
    for (Address server : this.servers) {
      send(request, server);
    }
  }
}
