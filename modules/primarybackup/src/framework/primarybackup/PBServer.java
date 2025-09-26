package framework.primarybackup;

import atmostonce.AMOApplication;
import atmostonce.AMOResult;
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
    if (isTransferOngoing) {
      System.out.println("PBServer.handleRequest: transfer ongoing case");
      System.exit(5414);
    }

    if (!iAmPrimary(m.view()) || !m.view().equals(this.view)) {
      System.out.println("PBServer.handleRequest: not primary in request view. or views dont match");
      System.exit(5416);
    }

    // at this point, I (the server) am the primary and received a request with matching view,
    // and I have no ongoing state transfer. can then proceed with the operation
    if (this.amoApplication.alreadyExecuted(m.command())) {
      System.out.println("PBServer.handleRequest: already executed command. send reply back");
      System.exit(5420);
    }

    if (this.view.backup() == null) {
      AMOResult amoResult = this.amoApplication.execute(m.command());
      send(new Reply(amoResult, this.view), sender);
    } else {
      System.out.println("PBServer.handleRequest: forward to backup case");
      System.exit(6313);
    }

  }

  /**
   * View Change Rule:
   * - If I am idle in the new view, don’t change (doesn’t matter)
   * - If I am primary in the new view, only change after receiving StateTransferAck with
   *   new view OR new view contains no backup (meaning no state transfer is necessary)
   * - If I am backup in the new view, only change after StateTransfer is received containing new view
   */
  private void handleViewReply(ViewReply m, Address sender) {
    assert sender.equals(this.viewServer);

    System.out.println("View Reply!!! view num is " + m.view().viewNum());
    View viewNew = m.view();

    if (isTransferOngoing) {
      System.out.println("handleViewReply: handle state transfer ongoing case");
      System.exit(2152);
    }

    // only primary will perform action upon receipt of a new view from VS
    if (viewNew.viewNum() > this.view.viewNum() && iAmPrimary(viewNew)) {
      if (viewNew.backup() != null) {
        initStateTransfer(viewNew);
      } else {
        assert !iAmPrimary(this.view) || this.view.viewNum() + 1 == viewNew.viewNum();
        this.view = viewNew;
      }
    }
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

  private void initStateTransfer(View viewNew) {
    assert iAmPrimary(viewNew) && viewNew.backup() != null;
    assert !isTransferOngoing;
    System.out.println("initStateTransfer: unimplemented");
    System.exit(4414);
    isTransferOngoing = true;
  }

  private boolean iAmPrimary(View view) {
    return view.primary() != null && view.primary().equals(this.address());
  }

  private boolean iAmBackup(View view) {
    return view.backup() != null && view.backup().equals(this.address());
  }

  private boolean iAmIdle(View view) {
    return !iAmPrimary(view) && !iAmBackup(view);
  }
}
