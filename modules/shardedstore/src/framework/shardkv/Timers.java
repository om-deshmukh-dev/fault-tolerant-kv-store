package framework.shardkv;

import framework.Timer;
import framework.paxos.PaxosRequest;
import lombok.Data;

@Data
final class ClientTimer implements Timer {
  static final int CLIENT_RETRY_MILLIS = 100;
  private final ShardStoreRequest request;
}

@Data
final class ClientQueryTimer implements Timer {
  static final int CLIENT_QUERY_RETRY_MILLIS = 100;
}

// Your code here...
