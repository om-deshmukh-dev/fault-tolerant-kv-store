package framework.pingpong;

import framework.Timeout;
import framework.pingpong.PingApplication.Ping;
import lombok.Data;

@Data
final class PingTimeout implements Timeout {
    private static final int PING_TIMEOUT_MILLIS = 10;

    private final Ping ping;

    @Override
    public int timeoutLengthMillis() {
        return PING_TIMEOUT_MILLIS;
    }
}
