package framework.shardkv;

import framework.atmostonce.AMOCommand;
import framework.Message;
import lombok.Data;

@Data
final class ShardStoreRequest implements Message {
  private final AMOCommand command;
}

@Data
final class ShardStoreReply implements Message {
  // Your code here...
}

// Your code here...
