package framework.atmostonce;

import framework.Result;
import lombok.Data;

@Data
public final class AMOResult implements Result {
  private final boolean wasSuccessfullyExecuted;
  private final Result result;

  // the current sequence number at the amoApp. if the command was
  // successfully executed, the client and server must have agreed on the
  // sequence number, and the client should get back the same sequence number
  // that they sent for their ongoing request(at most once semantics).
  private final int sequenceNum;
}
