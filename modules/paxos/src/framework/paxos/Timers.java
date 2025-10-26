package framework.paxos;

import framework.Timer;
import lombok.Data;

@Data
final class ClientTimer implements Timer {
  static final int CLIENT_RETRY_MILLIS = 50;
  private final PaxosRequest request;
}

@Data
final class HeartbeatTimer implements Timer {
  static final int HEARTBEAT_RETRY_MILLIS = 50;
}

@Data
final class HeartbeatCheckTimer implements Timer {
  static final int HEARTBEAT_CHECK_RETRY_MILLIS = 150;
}

@Data
final class ReproposeTimer implements Timer {
  static final int REPROPOSE_RETRY_MILLIS = 100;
}