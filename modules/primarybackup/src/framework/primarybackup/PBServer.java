package framework.primarybackup;

import atmostonce.AMOApplication;
import framework.Address;
import framework.Application;
import framework.Node;
import java.util.HashMap;
import lombok.EqualsAndHashCode;
import lombok.ToString;

@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
class PBServer extends Node {
  private final Address viewServer;
  private AMOApplication<Application> amoApplication;
  private View view;
  private boolean isTransferOngoing;

  /* -----------------------------------------------------------------------------------------------
   *  Construction and Initialization
   * ---------------------------------------------------------------------------------------------*/
  PBServer(Address address, Address viewServer, Application app) {
    super(address);
    this.viewServer = viewServer;
    this.amoApplication = new AMOApplication<>(app, new HashMap<>());
    this.view = new View(ViewServer.STARTUP_VIEWNUM, null, null);
    this.isTransferOngoing = false;
  }

  @Override
  public void init() {
    // set up pulsating timer to ping VS
    set(new PingTimer(), PingTimer.PING_MILLIS);
  }

  /* -----------------------------------------------------------------------------------------------
   *  Message Handlers
   * ---------------------------------------------------------------------------------------------*/
  private void handleRequest(Request m, Address sender) {
    // Your code here...
  }

  private void handleViewReply(ViewReply m, Address sender) {
    System.out.println("View Reply!!! view num is " + m.view().viewNum());
    System.exit(412);
  }

  // Your code here...

  /* -----------------------------------------------------------------------------------------------
   *  Timer Handlers
   * ---------------------------------------------------------------------------------------------*/
  private void onPingTimer(PingTimer t) {
    send(new Ping(this.view.viewNum()), this.viewServer);
    set(t, PingTimer.PING_MILLIS);
  }

  // Your code here...

  /* -----------------------------------------------------------------------------------------------
   *  Utils
   * ---------------------------------------------------------------------------------------------*/
  // Your code here...
}
