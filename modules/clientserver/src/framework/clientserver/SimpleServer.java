package framework.clientserver;

import framework.atmostonce.AMOApplication;
import framework.atmostonce.AMOResult;
import framework.Address;
import framework.Application;
import framework.Node;
import framework.Result;
import java.util.HashMap;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * Simple server that receives requests and returns responses.
 *
 * <p>See the documentation of {@link Node} for important implementation notes.
 */
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
class SimpleServer extends Node {
  private final AMOApplication<Application> amoApplication;

  /* -----------------------------------------------------------------------------------------------
   *  Construction and Initialization
   * ---------------------------------------------------------------------------------------------*/
  public SimpleServer(Address address, Application app) {
    super(address);
    this.amoApplication = new AMOApplication<>(app, new HashMap<>());
  }

  @Override
  public void init() {
    // No initialization necessary
  }

  /* -----------------------------------------------------------------------------------------------
   *  Message Handlers
   * ---------------------------------------------------------------------------------------------*/
  private void handleRequest(Request m, Address sender) {
    AMOResult result = this.amoApplication.execute(m.command());
    send(new Reply(result), sender);
  }
}
