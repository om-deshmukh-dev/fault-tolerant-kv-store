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
final class QueryTimer implements Timer {
  static final int QUERY_RETRY_MILLIS = 100;
}

@Data
final class ResendShardMovesTimer implements Timer {
  static final int RESEND_MILLIS = 100;
  private final int configNum;
}

@Data
final class DriveOngoingTransactionsTimer implements Timer {
  static final int DRIVE_TXN_MILLIS = 100;
}
