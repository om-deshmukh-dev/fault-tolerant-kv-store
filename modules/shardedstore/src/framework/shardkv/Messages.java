package framework.shardkv;

import framework.atmostonce.AMOCommand;
import framework.atmostonce.AMOResult;
import framework.Message;
import framework.shardkv.ShardStoreServer.ShardMove;
import framework.shardkv.ShardStoreServer.ShardMoveAck;
import framework.shardkv.ShardStoreServer.TPCCommit;
import framework.shardkv.ShardStoreServer.TPCCommitOk;
import framework.shardkv.ShardStoreServer.TPCPrepare;
import framework.shardkv.ShardStoreServer.TPCPrepareOk;
import framework.shardmaster.ShardMaster.ShardConfig;
import lombok.Data;

@Data
final class ShardStoreRequest implements Message {
  private final AMOCommand command;
}

@Data
final class ShardStoreReply implements Message {
  private final AMOResult result;
}

@Data
final class ShardStoreShardMove implements Message {
  private final ShardMove shardMove;
}

@Data
final class ShardStoreShardMoveAck implements Message {
  private final ShardMoveAck shardMoveAck;
}


// 2PC Messages

@Data
final class ShardStoreTPCPrepare implements  Message {
  private final TPCPrepare tpcPrepare;
}

@Data
final class ShardStoreTPCPrepareOk implements Message {
  private final TPCPrepareOk tpcPrepareOk;
}

@Data
final class ShardStoreTPCCommit implements Message {
  private final TPCCommit tpcCommit;
}

@Data
final class ShardStoreTPCCommitOk implements Message {
  private final TPCCommitOk tpcCommitOk;
}
