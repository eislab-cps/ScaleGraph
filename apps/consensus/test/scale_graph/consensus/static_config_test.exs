defmodule ScaleGraph.Consensus.StaticConfigTest do
  use ExUnit.Case
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
    assert :ok == StaticConfig.add(config, accountA, shardA)
    assert :ok == StaticConfig.add(config, accountB, shardB)
    assert StaticConfig.get(config, accountA) == shardA
    assert StaticConfig.get(config, accountB) == shardB
  end

end
