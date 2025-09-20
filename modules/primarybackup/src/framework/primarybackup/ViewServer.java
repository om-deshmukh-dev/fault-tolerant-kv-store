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

    if (canMoveOnToNewView) {
      attemptViewTransition();
    }
    set(t, PING_CHECK_MILLIS);
  }

  /* -----------------------------------------------------------------------------------------------
   *  Utils
   * ---------------------------------------------------------------------------------------------*/

  // handle primary/backup failure, or install new server as primary/backup in case of no failure
  private void attemptViewTransition() {
    if (isDead(view.primary()) && isDead(view.backup())) {
      // cry
    } else if (isDead(view.primary())) {
      changeView(view.backup(), null);
    } else if (isDead(view.backup())) {
      changeView(view.primary(), null);
    } else {
      tryInstallNewServer();
    }
  }

  private void tryInstallNewServer() {
    // only install a new server if an alive idle server exists
    if (!existsAliveIdleServer()) {
      return;
    }

    if (view.primary() == null && view.backup() == null) {
      changeView(selectAliveIdleServer(), null);
    }
    else if (view.backup() == null) {
      changeView(view.primary(), selectAliveIdleServer());
    }
  }

  // selects an alive idle server, assuming one exists
  private Address selectAliveIdleServer() {
    assert existsAliveIdleServer();

    for (Address server : currAliveServers) {
      if (!server.equals(view.primary()) && !server.equals(view.backup())) {
        return server;
      }
    }
    return null; // should not get here
  }

  private boolean existsAliveIdleServer() {
    return currAliveServers.stream().anyMatch((server) ->
        !server.equals(view.primary()) && !server.equals(view.backup()));
  }

  private void changeView(Address primary, Address backup) {
    int viewNumNext = (view.viewNum() == STARTUP_VIEWNUM) ? INITIAL_VIEWNUM : view.viewNum() + 1;
    view = new View(viewNumNext, primary, backup);
    canMoveOnToNewView = false; // must wait until primary acknowledges view
  }

  // if an actual server is not in the set of currently alive servers
  private boolean isDead(Address server) {
    return server != null && !currAliveServers.contains(server);
  }
}
