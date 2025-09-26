package framework.primarybackup;

import framework.Timer;
import lombok.Data;

@Data
final class PingCheckTimer implements Timer {
  static final int PING_CHECK_MILLIS = 100;
}

@Data
final class PingTimer implements Timer {
  static final int PING_MILLIS = 25;
}

@Data
final class ClientTimer implements Timer {
  static final int CLIENT_RETRY_MILLIS = 100;
  private final Request request;
}

@Data
final class ClientGetViewTimer implements Timer {
  static final int CLIENT_GET_VIEW_RETRY_MILLIS = 100;
}

// Your code here...
