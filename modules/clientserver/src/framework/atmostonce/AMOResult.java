package framework.atmostonce;

import framework.Result;
import lombok.Data;

@Data
public final class AMOResult implements Result {
  private final Result result;
  private final int sequenceNum;
}
