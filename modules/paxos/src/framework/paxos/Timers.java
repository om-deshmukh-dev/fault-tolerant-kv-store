package framework.paxos;

import framework.Timer;
import lombok.Data;

@Data
final class ClientTimer implements Timer {
  static final int CLIENT_RETRY_MILLIS = 100;
  private final PaxosRequest request;
}

// Your code here...
