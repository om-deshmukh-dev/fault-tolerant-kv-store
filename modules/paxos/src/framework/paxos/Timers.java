package framework.paxos;

import framework.Timer;
import lombok.Data;

@Data
final class ClientTimer implements Timer {
  static final int CLIENT_RETRY_MILLIS = 100;
  private final PaxosRequest request;
}

@Data
final class HeartbeatTimer implements Timer {
  static final int HEARTBEAT_RETRY_MILLIS = 25;
}

@Data
final class HeartbeatCheckTimer implements Timer {
  static final int HEARTBEAT_CHECK_RETRY_MILLIS = 100;
}
