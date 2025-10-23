package framework.paxos;

import framework.atmostonce.AMOCommand;
import framework.Message;
import framework.paxos.PaxosServer.Ballot;
import framework.paxos.PaxosServer.LogEntry;
import framework.paxos.PaxosServer.PValue;
import java.util.HashMap;
import lombok.Data;

@Data
final class P1a implements Message {
  private final Ballot ballot;
}

@Data
final class P1b implements Message {
  private final Ballot ballot;
  private final HashMap<Integer, LogEntry> log;
}

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