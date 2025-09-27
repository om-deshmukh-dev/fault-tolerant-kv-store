package framework.primarybackup;

import atmostonce.AMOCommand;
import atmostonce.AMOResult;
import framework.Address;
import framework.Client;
import framework.Command;
import framework.Node;
import framework.Result;
import lombok.EqualsAndHashCode;
import lombok.ToString;

@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
class PBClient extends Node implements Client {
  private final Address viewServer;
  private int sequenceNum; // for determining if the reply matches the ongoing request
  private Result result; // for getResult()
  private View view;

  // Your code here...

  /* -----------------------------------------------------------------------------------------------
   *  Construction and Initialization
   * ---------------------------------------------------------------------------------------------*/
  public PBClient(Address address, Address viewServer) {
    super(address);
    this.viewServer = viewServer;
    this.result = null;
    this.sequenceNum = 0;
    this.view = new View(ViewServer.STARTUP_VIEWNUM, null, null);
  }

  @Override
  public synchronized void init() {
    // setup pulsating timer to get view
    // TODO: remove this (unnecessary)
    set(new ClientGetViewTimer(), ClientGetViewTimer.CLIENT_GET_VIEW_RETRY_MILLIS);
  }

  /* -----------------------------------------------------------------------------------------------
   *  Client Methods
   * ---------------------------------------------------------------------------------------------*/
  @Override
  public synchronized void sendCommand(Command command) {
    AMOCommand amoCommand = new AMOCommand(command, this.address(), this.sequenceNum);
    Request request = new Request(amoCommand, this.view);
    this.result = null;

    sendRequestToPrimary(amoCommand);

    set(new ClientTimer(request), ClientTimer.CLIENT_RETRY_MILLIS);
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

    // client does not care if their view number is larger than the one in the ongoing request.
    // client can be assured that if both primary and backup in older view processed request, then
    // the effect of their request remains in all newer views
    if (amoResult.sequenceNum() == this.sequenceNum) {
      this.result = amoResult.result();
      this.sequenceNum++;
      notify();
    }
  }

  private synchronized void handleViewReply(ViewReply m, Address sender) {
    assert sender.equals(this.viewServer);
    if (m.view().viewNum() > this.view.viewNum()) {
      this.view = m.view();
    }
  }

  /* -----------------------------------------------------------------------------------------------
   *  Timer Handlers
   * ---------------------------------------------------------------------------------------------*/
  private synchronized void onClientTimer(ClientTimer t) {
    if (t.request().command().sequenceNum() == this.sequenceNum) {
      sendRequestToPrimary(t.request().command());
      set(t, ClientTimer.CLIENT_RETRY_MILLIS);
    }
  }

  private synchronized void onClientGetViewTimer(ClientGetViewTimer t) {
    send(new GetView(), this.viewServer);
    set(new ClientGetViewTimer(), ClientGetViewTimer.CLIENT_GET_VIEW_RETRY_MILLIS);
  }

  /* -----------------------------------------------------------------------------------------------
   *  Utils
   * ---------------------------------------------------------------------------------------------*/
  private void sendRequestToPrimary(AMOCommand command) {
    if (this.view.primary() != null) {
      send(new Request(command, this.view), this.view.primary());
    }
  }
}
