package framework.paxos;

import framework.atmostonce.AMOResult;
import framework.Message;
import lombok.Data;

@Data
public final class PaxosReply implements Message {
  private final AMOResult result;
}
