package framework.pingpong;

import framework.Timer;
import framework.pingpong.PingApplication.Ping;
import lombok.Data;

@Data
final class PingTimer implements Timer {
  static final int RETRY_MILLIS = 10;
  private final Ping ping;
}
