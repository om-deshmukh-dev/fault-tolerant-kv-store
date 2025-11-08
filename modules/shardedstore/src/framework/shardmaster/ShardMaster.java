package framework.shardmaster;

import framework.Address;
import framework.Application;
import framework.Command;
import framework.Result;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

@ToString
@EqualsAndHashCode
public final class ShardMaster implements Application {
  public static final int INVALID_CONFIG_NUM = -1;
  public static final int INITIAL_CONFIG_NUM = 0;

  private final int numShards;

  //                config ->   (group   ->   (group members, shards))
  private final Map<Integer, Map<Integer, Pair<Set<Address>, Set<Integer>>>> config;

  public ShardMaster(int numShards) {
    this.numShards = numShards;
    this.config = new HashMap<>();
  }

  public interface ShardMasterCommand extends Command {}

  @Data
  public static final class Join implements ShardMasterCommand {
    private final int groupId;
    private final Set<Address> servers;
  }

  @Data
  public static final class Leave implements ShardMasterCommand {
    private final int groupId;
  }

  @Data
  public static final class Move implements ShardMasterCommand {
    private final int groupId;
    private final int shardNum;
  }

  @Data
  public static final class Query implements ShardMasterCommand {
    private final int configNum;

    @Override
    public boolean readOnly() {
      return true;
    }
  }

  public interface ShardMasterResult extends Result {}

  @Data
  public static final class Ok implements ShardMasterResult {}

  @Data
  public static final class Error implements ShardMasterResult {}

  @Data
  public static final class ShardConfig implements ShardMasterResult {
    private final int configNum;

    // groupId -> <group members, shard numbers>
    private final Map<Integer, Pair<Set<Address>, Set<Integer>>> groupInfo;
  }

  @Override
  public Result execute(Command command) {
    if (command instanceof Join join) {
      return handleJoin(join.groupId, join.servers);
    }

    if (command instanceof Leave leave) {
      return handleLeave(leave.groupId());
    }

    if (command instanceof Move move) {
      return handleMove(move.groupId(), move.shardNum());
    }

    if (command instanceof Query query) {
      return handleQuery(query.configNum());
    }

    throw new IllegalArgumentException();
  }

  /**
   * Query Handler:
   */
  private Result handleQuery(int configNumQuery) {
    if (this.config.isEmpty()) {
      return new Error();
    }
    else if (this.config.containsKey(configNumQuery)) {
      return new ShardConfig(configNumQuery, this.config.get(configNumQuery));
    }
    else {
      int configNumLatest = getLatestConfigNum();
      assertWithThrow(configNumLatest != INVALID_CONFIG_NUM && this.config.containsKey(configNumLatest));
      return new ShardConfig(configNumLatest, this.config.get(configNumLatest));
    }
  }

  /**
   * Move Handler:
   */
  private Result handleMove(int groupId, int shardNum) {
    if (this.config.isEmpty()) { return new Error(); }
    if (shardNum <= 0 || shardNum > this.numShards) { return new Error(); }

    int configNumBeforeCopy = getLatestConfigNum();
    Map<Integer, Pair<Set<Address>, Set<Integer>>> groupInfoBeforeCopy = this.config.get(configNumBeforeCopy);

    // group not yet joined, or already managing shard
    if (!groupInfoBeforeCopy.containsKey(groupId)) {
      return new Error();
    } if (groupInfoBeforeCopy.get(groupId).getRight().contains(shardNum)) {
      return new Error();
    }

    // some other group must be managing shard, find them and move to this group
    int configNumAfterCopy = deepCopyToNewConfig();
    Map<Integer, Pair<Set<Address>, Set<Integer>>> groupInfoAfterCopy = this.config.get(configNumAfterCopy);

    for (Integer groupIdIter : groupInfoAfterCopy.keySet()) {
      if (groupInfoAfterCopy.get(groupIdIter).getRight().contains(shardNum)) {
        moveShard(
            groupInfoAfterCopy.get(groupIdIter).getRight(),
            groupInfoAfterCopy.get(groupId).getRight()
        );
        return new Ok();
      }
    }

    // should never get here (that means no one is managing shard)
    throw new IllegalArgumentException();
  }

  /**
   * Join Handler:
   */
  private Result handleJoin(int groupId, Set<Address> servers) {
    int configNumBeforeCopy = getLatestConfigNum();

    if (configNumBeforeCopy == INVALID_CONFIG_NUM) {
      // first join; initialize the shards this group is managing to the full set
      int configNumAfterCopy = deepCopyToNewConfig();
      assertWithThrow(configNumAfterCopy == INITIAL_CONFIG_NUM);
      this.config.get(configNumAfterCopy).put(
          groupId, new ImmutablePair<>(servers, initFullSetShards())
      );
      return new Ok();
    }
    else if (this.config.get(configNumBeforeCopy).containsKey(groupId)) {
      // groupId already exists in latest config
      return new Error();
    }
    else {
      // groupId not already in latest config; let new group have zero shards, then rebalance
      int configNumAfterCopy = deepCopyToNewConfig();
      assertWithThrow(configNumAfterCopy == configNumBeforeCopy + 1);

      this.config.get(configNumAfterCopy).put(groupId, new ImmutablePair<>(servers, new HashSet<>()));
      doTheRebalance();
      return new Ok();
    }
  }

  /**
   * Leave Handler:
   */
  private Result handleLeave(int groupId) {
    if (this.config.isEmpty()) { return new Error(); }

    int configNumBeforeCopy = getLatestConfigNum();
    Map<Integer, Pair<Set<Address>, Set<Integer>>> groupInfoBeforeCopy = this.config.get(configNumBeforeCopy);

    if (!groupInfoBeforeCopy.containsKey(groupId) || groupInfoBeforeCopy.size() == 1) {
      return new Error();
    }

    int configNumAfterCopy = deepCopyToNewConfig();
    Map<Integer, Pair<Set<Address>, Set<Integer>>> groupInfoAfterCopy = this.config.get(configNumAfterCopy);
    assertWithThrow(groupInfoAfterCopy.containsKey(groupId));

    // remove group from latest configuration, and redistribute their shards
    Set<Integer> shardsGroupLeft = groupInfoAfterCopy.get(groupId).getRight();
    groupInfoAfterCopy.remove(groupId);

    // find the most starved group for each shard, and give it to them
    for (Integer shard : shardsGroupLeft) {
      int groupIdMinShards = getGroupMinShards();
      Set<Integer> shardsGroupMin = groupInfoAfterCopy.get(groupIdMinShards).getRight();

      assertWithThrow(shardsGroupMin.size() < maxPermissibleShardsInGroup());
      assertWithThrow(!shardsGroupMin.contains(shard));

      shardsGroupMin.add(shard);
    }

    doTheRebalance();
    return new Ok();
  }

  /**
   * Helpers:
   */
  // Rebalance the shards in the latest configuration among all the groups, so that every group
  // is within one shard of every other group.
  // It is assumed that the configuration is not empty, and that the latest configuration has
  // at least one server.
  private void doTheRebalance() {
    int configNumLatest = getLatestConfigNum();

    assertWithThrow(!this.config.isEmpty() && this.config.containsKey(configNumLatest));
    assertWithThrow(configNumLatest > INITIAL_CONFIG_NUM);
    assertWithThrow(!this.config.get(configNumLatest).isEmpty());

    Map<Integer, Pair<Set<Address>, Set<Integer>>> groupInfoLatest = this.config.get(configNumLatest);

    for (Integer groupIdIter : groupInfoLatest.keySet()) {
      Set<Integer> shardsGroupIter = groupInfoLatest.get(groupIdIter).getRight();

      while (shardsGroupIter.size() < minPermissibleShardsInGroup()) {
        int groupIdMaxShards = getGroupMaxShards();
        Set<Integer> shardsGroupMax = groupInfoLatest.get(groupIdMaxShards).getRight();

        assertWithThrow(groupIdMaxShards != groupIdIter);
        assertWithThrow(shardsGroupMax.size() > minPermissibleShardsInGroup());

        moveShard(shardsGroupMax, shardsGroupIter);
      }
    }
  }

  // find and return the group with the smallest number of shards
  private int getGroupMinShards() {
    assertWithThrow(getLatestConfigNum() >= INITIAL_CONFIG_NUM);
    assertWithThrow(!this.config.get(getLatestConfigNum()).isEmpty());

    Map<Integer, Pair<Set<Address>, Set<Integer>>> groupInfoLatest = this.config.get(getLatestConfigNum());
    int groupIdMinSeen = -1;
    int sizeShardsMinSeen = this.numShards + 1;

    for (Integer groupIdIter : groupInfoLatest.keySet()) {
      int sizeShardsGroupIter = groupInfoLatest.get(groupIdIter).getRight().size();

      if (sizeShardsGroupIter < sizeShardsMinSeen) {
        sizeShardsMinSeen = sizeShardsGroupIter;
        groupIdMinSeen = groupIdIter;
      }
    }

    assertWithThrow(groupIdMinSeen >= 0);
    return groupIdMinSeen;
  }

  // find and return the group with the greatest number of shards.
  // assumes that the configuration is not empty.
  private int getGroupMaxShards() {
    assertWithThrow(getLatestConfigNum() >= INITIAL_CONFIG_NUM);
    assertWithThrow(!this.config.get(getLatestConfigNum()).isEmpty());

    // this type is very ugly...
    Map<Integer, Pair<Set<Address>, Set<Integer>>> groupInfoLatest = this.config.get(getLatestConfigNum());
    int groupIdMaxSeen = -1;
    int sizeShardsMaxSeen = 0;

    for (Integer groupIdIter : groupInfoLatest.keySet()) {
      int sizeShardsGroupIter = groupInfoLatest.get(groupIdIter).getRight().size();

      if (sizeShardsGroupIter > sizeShardsMaxSeen) {
        sizeShardsMaxSeen = sizeShardsGroupIter;
        groupIdMaxSeen = groupIdIter;
      }
    }

    assertWithThrow(groupIdMaxSeen >= 0);
    return groupIdMaxSeen;
  }

  // move one shard (to make it deterministic, move the minimum)
  private void moveShard(Set<Integer> s1, Set<Integer> s2) {
    Optional<Integer> minShardOpt = s1.stream().min(Integer::compareTo);
    assertWithThrow(minShardOpt.isPresent());

    s1.remove(minShardOpt.get());
    s2.add(minShardOpt.get());
  }

  // deep copy the current configuration, and put it a configuration number
  // one higher than the current latest config number. If the config map is empty,
  // will create a default configuration.
  // Returns the configuration number where the copy was placed.
  private int deepCopyToNewConfig() {
    Map<Integer, Pair<Set<Address>, Set<Integer>>> groupInfoNew = new HashMap<>();
    int configNumLatest = getLatestConfigNum();

    if (configNumLatest == INVALID_CONFIG_NUM) {
      this.config.put(INITIAL_CONFIG_NUM, groupInfoNew);
      return INITIAL_CONFIG_NUM;
    }

    Map<Integer, Pair<Set<Address>, Set<Integer>>> groupInfoOld = this.config.get(configNumLatest);

    // create a deep copy of the old groupInfo object
    for (Integer groupId : groupInfoOld.keySet()) {
      groupInfoNew.put(groupId, new ImmutablePair<>(
          new HashSet<>(groupInfoOld.get(groupId).getLeft()),
          new HashSet<>(groupInfoOld.get(groupId).getRight())
      ));
    }

    this.config.put(configNumLatest + 1, groupInfoNew);
    return configNumLatest + 1;
  }

  private Set<Integer> initFullSetShards() {
    Set<Integer> shards = new HashSet<>();
    for (int shard = 1; shard <= this.numShards; shard++) {
      shards.add(shard);
    }
    return shards;
  }

  private int minPermissibleShardsInGroup() {
    int configNumLatest = getLatestConfigNum();

    // latest config must exist, and map at least one group
    assertWithThrow(configNumLatest != INVALID_CONFIG_NUM);
    assertWithThrow(this.config.get(configNumLatest) != null && !this.config.get(configNumLatest).isEmpty());

    return (int)Math.floor((double)this.numShards / this.config.get(configNumLatest).size());
  }

  private int maxPermissibleShardsInGroup() {
    int configNumLatest = getLatestConfigNum();

    // latest config must exist, and map at least one group
    assertWithThrow(configNumLatest != INVALID_CONFIG_NUM);
    assertWithThrow(this.config.get(configNumLatest) != null && !this.config.get(configNumLatest).isEmpty());

    return (int)Math.ceil((double)this.numShards / this.config.get(configNumLatest).size());
  }

  // gets the latest config number in the configuration,
  // or returns -1 if no configuration exists
  private int getLatestConfigNum() {
    int maxConfigNum = INVALID_CONFIG_NUM;
    for (Integer configNum : config.keySet()) {
      maxConfigNum = Math.max(maxConfigNum, configNum);
    }
    return maxConfigNum;
  }

  private void assertWithThrow(boolean b) {
    if (!b) {
      throw new RuntimeException();
    }
  }
}
