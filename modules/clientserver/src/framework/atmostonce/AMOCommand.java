package framework.atmostonce;

import framework.Command;
import lombok.Data;

@Data
public final class AMOCommand implements Command {
  private final Command command;

  // sequence number maintained by the client. the amoApp will only
  // execute commands where the sequence number matches, in effect to
  // disallow old requests from being executed.
  private final int sequenceNum;
}
