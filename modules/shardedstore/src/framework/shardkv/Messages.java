package framework.shardkv;

import framework.atmostonce.AMOCommand;
import framework.atmostonce.AMOResult;
import framework.Message;
import framework.shardkv.ShardStoreServer.ShardMove;
import framework.shardkv.ShardStoreServer.ShardMoveAck;
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
