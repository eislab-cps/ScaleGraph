defmodule ScaleGraph.Consensus.StaticConfigTest do
  use ExUnit.Case
  alias ScaleGraph.Consensus.ShardConfig
  alias ScaleGraph.Consensus.StaticConfig

  test "moop" do
    accountA = Crypto.sha256(123)
    accountB = Crypto.sha256(234)
    nodeID1 = Crypto.sha256(111)
    nodeID2 = Crypto.sha256(222)
    nodeID3 = Crypto.sha256(333)
    {:ok, config} = StaticConfig.start_link([])
    shardA = [nodeID1, nodeID2]
    shardB = [nodeID2, nodeID3]
    assert :ok == ShardConfig.add(config, accountA, shardA)
    assert :ok == ShardConfig.add(config, accountB, shardB)
    assert ShardConfig.get(config, accountA) == shardA
    assert ShardConfig.get(config, accountB) == shardB
  end

end
