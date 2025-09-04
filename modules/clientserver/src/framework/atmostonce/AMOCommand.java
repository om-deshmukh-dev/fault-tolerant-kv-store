package framework.atmostonce;

import framework.Address;
import framework.Command;
import lombok.Data;

@Data
public final class AMOCommand implements Command {
  private final Command command;

  // acts as a unique identifier for each client. server requires this identifier
  // to correctly determine which clients sent which application commands,
  // which is useful since different clients can send the same application command
  private final Address address;

  // acts as a unique identifier for each request that a client sends. the server
  // uses this identifier (instead of the actual `command`) to determine whether a client's
  // request has already been executed.
  private final int sequenceNum;
}
