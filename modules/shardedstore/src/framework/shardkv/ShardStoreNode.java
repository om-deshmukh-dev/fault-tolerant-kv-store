package framework.shardkv;

import framework.Address;
import framework.Message;
import framework.Node;
import framework.shardmaster.ShardMaster;
import framework.shardmaster.ShardMaster.ShardConfig;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;
import lombok.AccessLevel;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import org.apache.commons.lang3.tuple.Pair;

@EqualsAndHashCode(callSuper = true)
abstract class ShardStoreNode extends Node {
  @Getter(AccessLevel.PACKAGE)
  private final Address[] shardMasters;

  private final int numShards;

  ShardStoreNode(Address address, Address[] shardMasters, int numShards) {
    super(address);
    this.shardMasters = shardMasters;
    this.numShards = numShards;
  }

  void broadcastToShardMasters(Message message) {
    broadcast(message, shardMasters);
  }

  // Find the group in the current configuration managing `shardNum`.
  // It is required that exactly one group is managing this shard in the
  // latest configuration at the client.
  int getGroupIdForShard(@NonNull ShardConfig shardConfig, int shardNum) {
    for (Integer groupId : shardConfig.groupInfo().keySet()) {
      Pair<Set<Address>, Set<Integer>> groupMetadata = shardConfig.groupInfo().get(groupId);

      if (groupMetadata.getRight().contains(shardNum)) { return groupId; }
    }
    return -1;
  }

  Set<Address> getServersForGroupId(@NonNull ShardConfig shardConfig, int groupId) {
    if (shardConfig.groupInfo().containsKey(groupId)) {
      return shardConfig.groupInfo().get(groupId).getLeft();
    }
    return new HashSet<>();
  }

  /**
   * Returns the shard number for a given key when the system has numShards shards in total. The
   * shards are numbered 1..numShards (inclusive). When the key ends in \d+ (e.g. key-10), the shard
   * number is given by that number (e.g., 10 mod numShards). Otherwise, the shard number is the
   * hash value of the key (mod numShards).
   *
   * @param key the key
   * @param numShards the total number of shards in the system
   * @return the shard number of key (in 1..numShards inclusive)
   */
  static int keyToShard(@NonNull String key, int numShards) {
    LinkedList<Character> cl = new LinkedList<>();
    for (int i = key.length() - 1; i >= 0 && Character.isDigit(key.charAt(i)); i--) {
      cl.add(key.charAt(i));
    }
    Collections.reverse(cl);

    int hash = 0;
    if (!cl.isEmpty()) {
      for (char c : cl) {
        hash = hash * 10 + Character.getNumericValue(c);
      }
    } else {
      hash = key.hashCode();
    }

    int mod = hash % numShards;
    if (mod <= 0) {
      mod += numShards;
    }
    return mod;
  }

  /**
   * @see #keyToShard(String, int)
   */
  int keyToShard(String key) {
    return keyToShard(key, numShards);
  }
}
