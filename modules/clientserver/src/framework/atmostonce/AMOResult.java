package framework.atmostonce;

import framework.Result;
import lombok.Data;

@Data
public final class AMOResult implements Result {
  private final Result result;
  // client checks sequence number in response to determine if response is meant for ongoing request
  private final int sequenceNum;
}
