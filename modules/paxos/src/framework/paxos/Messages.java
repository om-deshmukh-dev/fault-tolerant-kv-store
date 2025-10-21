package framework.paxos;

import framework.atmostonce.AMOCommand;
import framework.Message;
import framework.paxos.PaxosServer.Ballot;
import framework.paxos.PaxosServer.PValue;
import lombok.Data;

@Data
final class P2a implements Message {
  private final PValue pValue;
}

@Data
final class P2b implements Message {
  private final PValue pValue;
}

@Data
final class Decision implements Message {
  private final PValue pValue;
}