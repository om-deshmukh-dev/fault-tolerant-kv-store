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
  private void handleRequest(Request request, Address sender) {
    if (isTransferOngoing) {
      // System.out.println("PBServer.handleRequest: transfer ongoing case");
      return;
    }

    if (!iAmPrimary(request.view()) || !request.view().equals(this.view)) {
      // TODO: for an optimization, can initiate state transfer here if the request view is higher
      // TODO: and this server is the primary in the new view (don't have to wait for VS ViewReply)
      // System.out.println("PBServer.handleRequest: not primary in request view. or views dont match " + request.view() + " this view " + this.view + " this address " + this.address());
      return;
    }

    // at this point, I (the server) am the primary and received a request with matching view,
    // and I have no ongoing state transfer. can then proceed with the operation
    if (this.amoApplication.alreadyExecuted(request.command())) {
      // System.out.println("PBServer.handleRequest: already executed command. send reply back");
      AMOResult amoResult = this.amoApplication.execute(request.command());
      send(new Reply(amoResult, this.view), sender);
    }

    if (this.view.backup() == null) {
      AMOResult amoResult = this.amoApplication.execute(request.command());
      send(new Reply(amoResult, this.view), sender);
    } else {
      // at this point, the backup in the request and the view match
      send(new Forward(request, sender), this.view.backup());
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
    View viewNew = m.view();

    if (isTransferOngoing) {
      // System.out.println("handleViewReply: handle state transfer ongoing case");
      return;
    }

    // only primary will perform action upon receipt of a new view from VS
    if (viewNew.viewNum() > this.view.viewNum() && iAmPrimary(viewNew)) {
      assert this.view.viewNum() + 1 == viewNew.viewNum();

      if (viewNew.backup() != null) {
        initStateTransfer(viewNew);
      } else {
        this.view = viewNew;
      }
    }
  }

  private void handleStateTransfer(StateTransfer stateTransfer, Address sender) {
    if (!iAmBackup(stateTransfer.view())) {
      // System.out.println("PBServer.handleStateTransfer: not backup in new view");
      return;
    }
    assert !isTransferOngoing;
    assert sender.equals(stateTransfer.view().primary());

    if (stateTransfer.view().viewNum() > this.view.viewNum()) {
      // newer view: transfer application state, update view, and send ack
      this.amoApplication = stateTransfer.amoApplication();
      this.view = stateTransfer.view();
      send(new StateTransferAck(this.view), sender);
    }
    else if (stateTransfer.view().viewNum() == this.view.viewNum()) {
      // current view: transfer already applied, can send back ack immediately
      send(new StateTransferAck(this.view), sender);
    }
    else {
      // older view: don't care (TODO: think about this more)
      // System.out.println("PBServer.handleStateTransfer: state transfer contains old view");
    }
  }

  private void handleStateTransferAck(StateTransferAck stateTransferAck, Address sender) {
    if (stateTransferAck.view().viewNum() > this.view.viewNum()) {
      assert stateTransferAck.view().primary().equals(this.address());
      assert stateTransferAck.view().backup().equals(sender);
      // The below assertion must hold if we do casework on why the state transfer happened:
      //  1. primary installed new backup: then primary must have acknowledged current view for
      //                                   VS to move on, so primary must only be one behind
      //  2. backup promoted to primary, and non-null new backup: backup must have set their view
      //                                                          to the one in the previous
      //                                                          StateTransfer they got.
      assert this.view.viewNum() + 1 == stateTransferAck.view().viewNum();

      this.view = stateTransferAck.view();
      this.isTransferOngoing = false;
    }
  }

  private void handleForward(Forward forward, Address sender) {
    if (iAmBackup(forward.request().view()) && forward.request().view().equals(this.view)) {
      assert sender.equals(this.view.primary());
      assert this.view.backup().equals(this.address());
      assert !isTransferOngoing;

      amoApplication.execute(forward.request().command());
      send(new ForwardAck(forward.request(), forward.client()), sender);
    }
    else {
      // System.out.println("PBServer.handleForward: i am not backup or views dont match");
    }
  }

  private void handleForwardAck(ForwardAck forwardAck, Address sender) {
    if (isTransferOngoing) {
      // System.out.println("PBServer.handleForwardAck: transfer ongoing (blocked)");
      return;
    }

    if (forwardAck.request().view().equals(this.view)) {
      assert iAmPrimary(this.view);
      assert !amoApplication.alreadyExecuted(forwardAck.request().command());

      AMOResult result = amoApplication.execute(forwardAck.request().command());
      send(new Reply(result, this.view), forwardAck.client());
    }
  }


  /* -----------------------------------------------------------------------------------------------
   *  Timer Handlers
   * ---------------------------------------------------------------------------------------------*/
  private void onPingTimer(PingTimer t) {
    send(new Ping(this.view.viewNum()), this.viewServer);
    set(t, PingTimer.PING_MILLIS);
  }

  private void onStateTransferTimer(StateTransferTimer t) {
    if (t.stateTransfer().view().viewNum() > this.view.viewNum()) {
      assert isTransferOngoing;
      assert t.stateTransfer().view().primary().equals(this.address());
      send(t.stateTransfer(), t.stateTransfer().view().backup());
      set(t, StateTransferTimer.STATE_TRANSFER_RETRY_MILLIS);
    }
  }

  /* -----------------------------------------------------------------------------------------------
   *  Utils
   * ---------------------------------------------------------------------------------------------*/

  private void initStateTransfer(View viewNew) {
    assert iAmPrimary(viewNew) && viewNew.backup() != null;
    assert viewNew.viewNum() > this.view.viewNum();
    assert !isTransferOngoing;

    // state transfer contains NEW view (not the one at primary)
    StateTransfer stateTransfer = new StateTransfer(this.amoApplication, viewNew);

    send(stateTransfer, viewNew.backup());
    set(new StateTransferTimer(stateTransfer), StateTransferTimer.STATE_TRANSFER_RETRY_MILLIS);
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

  private void sendErrorBack(Address sender) {
    // TODO: implement eventually
  }
}
