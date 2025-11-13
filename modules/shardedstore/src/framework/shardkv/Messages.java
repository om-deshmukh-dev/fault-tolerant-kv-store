package framework.shardkv;

import framework.atmostonce.AMOCommand;
import framework.atmostonce.AMOResult;
import framework.Message;
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
