package framework.atmostonce;

import framework.Address;
import framework.Command;
import lombok.Data;

@Data
public final class AMOCommand implements Command {
  private final Command command;
  private final Address address; // who sent the command
  private final int sequenceNum; // echo back
}
