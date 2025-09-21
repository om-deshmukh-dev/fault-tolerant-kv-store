package framework.primarybackup;

import static framework.primarybackup.PingCheckTimer.PING_CHECK_MILLIS;

import framework.Address;
import framework.Node;
import java.util.HashMap;
import java.util.HashSet;
import lombok.EqualsAndHashCode;
import lombok.ToString;

@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
class ViewServer extends Node {
  static final int STARTUP_VIEWNUM = 0;
  private static final int INITIAL_VIEWNUM = 1;

  // the latest view at the view server. Once the primary in this view has acknowledged it by
  // sending a ping with the same view number, the view server is able to aggressively move on
  // to another view due to primary/backup failure or primary/backup installation.
  private View view;
  private boolean canMoveOnToNewView;

  private HashSet<Address> currAliveServers; // servers that are currently alive before next timer goes off
  private HashSet<Address> nextAliveServers; // servers that are alive after next timer goes off

  /* -----------------------------------------------------------------------------------------------
   *  Construction and Initialization
   * ---------------------------------------------------------------------------------------------*/
  public ViewServer(Address address) {
    super(address);
  }

  @Override
  public void init() {
    view = new View(STARTUP_VIEWNUM, null, null);
    canMoveOnToNewView = true;

    currAliveServers = new HashSet<>();
    nextAliveServers = new HashSet<>();

    set(new PingCheckTimer(), PING_CHECK_MILLIS);
  }

  /* -----------------------------------------------------------------------------------------------
   *  Message Handlers
   * ---------------------------------------------------------------------------------------------*/
  private void handlePing(Ping m, Address sender) {
    // don't care what their view number is, or whether the ping is duplicated
    nextAliveServers.add(sender);
    canMoveOnToNewView |= (sender.equals(view.primary()) && m.viewNum() == view.viewNum());

    if (canMoveOnToNewView) {
      tryPrematureInstallIdleServer();
    }
    send(new ViewReply(view), sender);
  }

  private void handleGetView(GetView m, Address sender) {
    send(new ViewReply(this.view), sender);
  }

  /* -----------------------------------------------------------------------------------------------
   *  Timer Handlers
   * ---------------------------------------------------------------------------------------------*/
  private void onPingCheckTimer(PingCheckTimer t) {
    // servers that pinged in previous time interval are now seen as "alive"
    currAliveServers = nextAliveServers;
    nextAliveServers = new HashSet<>();

    if (canMoveOnToNewView && (isDead(view.primary()) || isDead(view.backup()))) {
      handlePrimaryBackupFailure();
    }
    set(t, PING_CHECK_MILLIS);
  }

  /* -----------------------------------------------------------------------------------------------
   *  Utils
   * ---------------------------------------------------------------------------------------------*/

  /**
   * Attempt to move on to new view if there is an idle server that has pinged but is not yet alive,
   * and a primary/backup needs to be installed. Is premature since the idle server is not yet alive.
   */
  private void tryPrematureInstallIdleServer() {
    assert canMoveOnToNewView;

    // only install a new server if an alive idle server exists
    if (!existsIdleServer(nextAliveServers)) { return; }

    if (view.primary() == null && view.backup() == null) {
      changeView(selectIdleServer(nextAliveServers), null);
    } else if (view.backup() == null) {
      changeView(view.primary(), selectIdleServer(nextAliveServers));
    }
  }

  /**
   * Handle primary/backup failure:
   *  - primary failure: transition to new view with a new idle server as backup (or null if none found)
   *  - backup failure: transition to new view with backup as primary and idle server as new backup (or null if none found)
   */
  private void handlePrimaryBackupFailure() {
    assert canMoveOnToNewView;

    // primary failure where backup can replace it
    if (isDead(view.primary()) && view.backup() != null && !isDead(view.backup())) {
      changeView(view.backup(), selectIdleServer(currAliveServers));
    }

    // backup failure
    if (isDead(view.backup())) {
      assert view.primary() != null;
      changeView(view.primary(), selectIdleServer(currAliveServers));
    }
  }


  /**
   * Selects an idle server from the set of servers passed in, or returns null if none exist
   */
  private Address selectIdleServer(HashSet<Address> servers) {
    for (Address server : servers) {
      if (isIdle(server)) {
        return server;
      }
    }
    return null;
  }

  private boolean existsIdleServer(HashSet<Address> servers) {
    return servers.stream().anyMatch(this::isIdle);
  }

  /**
   * Update the current view with an incremented view number, and new primary/backup.
   */
  private void changeView(Address primary, Address backup) {
    assert canMoveOnToNewView;

    int viewNumNext = (view.viewNum() == STARTUP_VIEWNUM) ? INITIAL_VIEWNUM : view.viewNum() + 1;
    view = new View(viewNumNext, primary, backup);
    canMoveOnToNewView = false; // must wait until primary acknowledges view
  }

  /**
   * Determines whether the server is not in the set of currently alive servers. Returns false
   * if null is passed in (only valid addresses can be defined as dead).
   */
  private boolean isDead(Address server) {
    return server != null && !currAliveServers.contains(server);
  }

  /**
   * Determines whether the server is idle, meaning that it that doesn't match the primary or
   * backup in the current view. Returns false if null is passed in (only valid addresses can be idle).
   */
  private boolean isIdle(Address server) {
    return server != null && !server.equals(view.primary()) && !server.equals(view.backup());
  }
}
