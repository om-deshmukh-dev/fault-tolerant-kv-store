package framework.paxos;

import framework.atmostonce.AMOCommand;
import framework.Message;
import framework.paxos.PaxosServer.Ballot;
import lombok.Data;

// Your code here...
@Data
final class P2a implements Message {
  private final Ballot ballot;
  private final int slotNum;
  private final AMOCommand command;
}