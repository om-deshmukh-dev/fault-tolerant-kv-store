package framework.paxos;

import framework.atmostonce.AMOCommand;
import framework.Message;
import lombok.Data;

@Data
public final class PaxosDecision implements Message {
  private final int slotNum; // the slot the command was chosen for (for assertions)
  private final AMOCommand amoCommand; // this is the command the ShardStoreServer should exec
}
