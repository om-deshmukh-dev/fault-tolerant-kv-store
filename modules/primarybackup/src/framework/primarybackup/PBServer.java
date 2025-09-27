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
  private Request requestForwardOngoing;

  /* -----------------------------------------------------------------------------------------------
   *  Construction and Initialization
   * ---------------------------------------------------------------------------------------------*/
  PBServer(Address address, Address viewServer, Application app) {
    super(address);
    this.viewServer = viewServer;
    this.amoApplication = new AMOApplication<>(app, new HashMap<>());
    this.view = new View(ViewServer.STARTUP_VIEWNUM, null, null);
    this.requestForwardOngoing = null;
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
      assert this.requestForwardOngoing == null; return;
    }
    if (!request.view().equals(this.view) || !iAmPrimary(request.view())) {
      // TODO: for an optimization, can initiate state transfer here if the request view is higher
      // TODO: and this server is the primary in the new view (don't have to wait for VS ViewReply)
      return;
    }
    // block if I (as the primary) am forwarding a different request
    if (isRequestForwardOngoing() && !this.requestForwardOngoing.equals(request)) {
      return;
    }

    // At this point, I am primary, views match, no state transfer, and no other client request.
    // Then I must process the request:
    if (canProcessWithoutForwarding(request)) {
      assert !isRequestForwardOngoing();
      AMOResult amoResult = this.amoApplication.execute(request.command());
      send(new Reply(amoResult, this.view), sender);
    } else {
      send(new Forward(request, sender), this.view.backup());
      this.requestForwardOngoing = request;
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
      return;
    }

    // only primary will perform action upon receipt of a new view from VS
    if (viewNew.viewNum() > this.view.viewNum() && iAmPrimary(viewNew)) {
      assert this.view.viewNum() + 1 == viewNew.viewNum();
      this.requestForwardOngoing = null; // don't care about ForwardAck from old backup

      if (viewNew.backup() != null) {
        initStateTransfer(viewNew);
      } else {
        this.view = viewNew;
      }
    }
  }

  private void handleStateTransfer(StateTransfer stateTransfer, Address sender) {
    // could be that backup can be promoted to primary, and get duplicated state transfer
    if (!iAmBackup(stateTransfer.view()) || isTransferOngoing) {
      return;
    }
    assert sender.equals(stateTransfer.view().primary());

    if (stateTransfer.view().viewNum() > this.view.viewNum()) {
      // newer view: transfer application state, update view, and send ack
      this.amoApplication = stateTransfer.amoApplication();
      this.view = stateTransfer.view();
      this.requestForwardOngoing = null; // to be safe
      send(new StateTransferAck(this.view), sender);
    }
    else if (stateTransfer.view().viewNum() == this.view.viewNum()) {
      // current view: transfer already applied, can send back ack immediately
      assert this.requestForwardOngoing == null;
      send(new StateTransferAck(this.view), sender);
    }
  }

  private void handleStateTransferAck(StateTransferAck stateTransferAck, Address sender) {
    if (stateTransferAck.view().viewNum() > this.view.viewNum()) {
      assert stateTransferAck.view().primary().equals(this.address());
      assert stateTransferAck.view().backup().equals(sender);
      assert !isRequestForwardOngoing();
      assert isTransferOngoing;
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
    // EXTREMELY IMPORTANT: otherwise may execute forward that is not included in state transfer
    if (isTransferOngoing) {
      return;
    }

    if (iAmBackup(forward.request().view()) && forward.request().view().equals(this.view)) {
      assert sender.equals(this.view.primary());
      assert this.view.backup().equals(this.address());
      assert !isRequestForwardOngoing();

      amoApplication.execute(forward.request().command());
      send(new ForwardAck(forward.request(), forward.client()), sender);
    }
  }

  private void handleForwardAck(ForwardAck forwardAck, Address sender) {
    if (isTransferOngoing || !isRequestForwardOngoing()) {
      return;
    }

    if (forwardAck.request().view().equals(this.view) && forwardAck.request().equals(this.requestForwardOngoing)) {
      assert iAmPrimary(this.view);

      AMOResult result = amoApplication.execute(forwardAck.request().command());
      send(new Reply(result, this.view), forwardAck.client());
      this.requestForwardOngoing = null;
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
      assert !isRequestForwardOngoing();
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
    assert requestForwardOngoing == null;

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

  private boolean isRequestForwardOngoing() {
    return requestForwardOngoing != null;
  }

  private boolean canProcessWithoutForwarding(Request request) {
    return this.amoApplication.alreadyExecuted(request.command()) || this.view.backup() == null;
  }

  private void sendErrorBack(Address sender) {
    // TODO: implement eventually
  }
}
