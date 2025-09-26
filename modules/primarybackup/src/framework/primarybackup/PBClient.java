package framework.primarybackup;

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
    set(new ClientGetViewTimer(), ClientGetViewTimer.CLIENT_GET_VIEW_RETRY_MILLIS);
  }

  /* -----------------------------------------------------------------------------------------------
   *  Client Methods
   * ---------------------------------------------------------------------------------------------*/
  @Override
  public synchronized void sendCommand(Command command) {
    // Your code here...
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
    // Your code here...
  }

  private synchronized void handleViewReply(ViewReply m, Address sender) {
    assert sender.equals(this.viewServer);
    if (m.view().viewNum() > this.view.viewNum()) {
      System.out.println("handleViewReply: FINALLY!!!");
      System.exit(543);
      this.view = m.view();
    }
  }

  // Your code here...

  /* -----------------------------------------------------------------------------------------------
   *  Timer Handlers
   * ---------------------------------------------------------------------------------------------*/
  private synchronized void onClientTimer(ClientTimer t) {
    // Your code here...
  }

  private synchronized void onClientGetViewTimer(ClientGetViewTimer t) {
    send(new GetView(), this.viewServer);
    set(new ClientGetViewTimer(), ClientGetViewTimer.CLIENT_GET_VIEW_RETRY_MILLIS);
  }
}
