package framework.pingpong;

import framework.Timeout;
import framework.pingpong.PingApplication.Ping;
import lombok.Data;

@Data
final class PingTimeout implements Timeout {
    static final int RETRY_MILLIS = 10;
    private final Ping ping;
}
