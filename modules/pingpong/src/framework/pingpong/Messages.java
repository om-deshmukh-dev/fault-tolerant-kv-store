package framework.pingpong;

import framework.Message;
import framework.pingpong.PingApplication.Ping;
import framework.pingpong.PingApplication.Pong;
import lombok.Data;

@Data
class PingRequest implements Message {
    private final Ping ping;
}

@Data
class PongReply implements Message {
    private final Pong pong;
}
