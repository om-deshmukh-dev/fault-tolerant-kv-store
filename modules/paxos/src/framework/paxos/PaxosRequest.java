package framework.paxos;

import framework.atmostonce.AMOCommand;
import framework.Message;
import lombok.Data;

@Data
public final class PaxosRequest implements Message {
  private final AMOCommand command;
}
