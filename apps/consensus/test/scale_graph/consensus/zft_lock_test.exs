defmodule ScaleGraph.Consensus.ZftLockTest do
  use ExUnit.Case
  alias ScaleGraph.Consensus
  alias Consensus.StaticConfig
  alias Consensus.ZftLock
  alias ScaleGraph.Ledger
  alias Ledger.Ledgers
  alias Ledger.Transaction
  alias Netsim.Fake

  # TODO: should be configurable
  @start_balance 1000

  defp instance_state(consensus, account) do
    consensus_state = :sys.get_state(consensus)
    zft_pid = consensus_state.instances[account]
    :sys.get_state(zft_pid)
  end

  setup do
    {:ok, ledgers1} = Ledgers.start_link([])
    {:ok, ledgers2} = Ledgers.start_link([])
    {:ok, ledgers3} = Ledgers.start_link([])
    {:ok, ledgers4} = Ledgers.start_link([])
    {:ok, configs} = Consensus.StaticConfig.start_link([])
    {:ok, net} = Fake.start_link([])
    %{
      accountA: Crypto.sha256(123),
      accountB: Crypto.sha256(234),
      accountC: Crypto.sha256(345),
      node1_id: Crypto.sha256(111),
      node2_id: Crypto.sha256(222),
      node3_id: Crypto.sha256(333),
      node4_id: Crypto.sha256(444),
      node1_addr: {{127, 0, 0, 1}, 10_001},
      node2_addr: {{127, 0, 0, 2}, 10_002},
      node3_addr: {{127, 0, 0, 3}, 10_003},
      node4_addr: {{127, 0, 0, 4}, 10_004},
      ledgers1: ledgers1,
      ledgers2: ledgers2,
      ledgers3: ledgers3,
      ledgers4: ledgers4,
      configs: configs,
      net: net,
    }
  end

  test "process_tx enqueues TX when not leader", context do
    %{
      accountA: accountA, accountB: accountB,
      node1_id: node1_id, node2_id: node2_id,
      node1_addr: node1_addr, node2_addr: node2_addr,
      ledgers1: ledgers1,
      configs: configs, net: net,
    } = context
    # Set up accounts and shards
    _ledger1A = Ledgers.create(ledgers1, accountA)
    shardA = [{node2_id, node2_addr}, {node1_id, node1_addr}]
    #_shardB = [{node2_id, node2_addr}, {node3_id, node3_addr}]
    assert :ok == StaticConfig.add(configs, accountA, shardA)
    {:ok, consensus1} = Consensus.start_link(node_id: node1_id, node_addr: node1_addr, ledgers: ledgers1, config_lookup: configs, netmod: Fake, net: net, protocol: ZftLock)
    tx1 = Transaction.new(accountA, accountB, 100, 0)
    Consensus.process_tx(consensus1, {tx1, nil})
    zft_state = instance_state(consensus1, accountA)
    assert zft_state.tx_queue == [{tx1, nil}]
  end

  test "process_tx proposes TX when leader", context do
    %{
      accountA: accountA, accountB: accountB,
      node1_id: node1_id, node2_id: node2_id, node3_id: node3_id,
      node1_addr: node1_addr, node2_addr: node2_addr, node3_addr: node3_addr,
      ledgers1: ledgers1,
      ledgers2: ledgers2,
      ledgers3: ledgers3,
      configs: configs, net: net,
    } = context
    # Set up accounts and shards
    ledger1A = Ledgers.create(ledgers1, accountA)
    ledger2A = Ledgers.create(ledgers2, accountA)
    ledger2B = Ledgers.create(ledgers2, accountB)
    ledger3B = Ledgers.create(ledgers3, accountB)
    shardA = [{node1_id, node1_addr}, {node2_id, node2_addr}]
    shardB = [{node2_id, node2_addr}, {node3_id, node3_addr}]
    assert :ok == StaticConfig.add(configs, accountA, shardA)
    assert :ok == StaticConfig.add(configs, accountB, shardB)
    {:ok, consensus1} = Consensus.start_link(node_id: node1_id, node_addr: node1_addr, ledgers: ledgers1, config_lookup: configs, netmod: Fake, net: net, protocol: ZftLock)
    {:ok, _consensus2} = Consensus.start_link(node_id: node2_id, node_addr: node2_addr, ledgers: ledgers2, config_lookup: configs, netmod: Fake, net: net, protocol: ZftLock)
    {:ok, _consensus3} = Consensus.start_link(node_id: node3_id, node_addr: node3_addr, ledgers: ledgers3, config_lookup: configs, netmod: Fake, net: net, protocol: ZftLock)
    tx1 = Transaction.new(accountA, accountB, 100, 0)
    Consensus.process_tx(consensus1, {tx1, nil})
    :timer.sleep(200)
    assert Ledger.account(ledger1A).balance == -100 + @start_balance
    assert Ledger.account(ledger2A).balance == -100 + @start_balance
    assert Ledger.account(ledger2B).balance == 100 + @start_balance
    assert Ledger.account(ledger3B).balance == 100 + @start_balance
  end

  test "single node, single TX", context do
    %{
      accountA: accountA, accountB: accountB,
      node1_id: node1_id,
      node1_addr: node1_addr,
      ledgers1: ledgers1,
      configs: configs, net: net,
    } = context
    # Set up accounts and shards
    ledger1A = Ledgers.create(ledgers1, accountA)
    ledger1B = Ledgers.create(ledgers1, accountB)
    shardA = [{node1_id, node1_addr}]
    shardB = [{node1_id, node1_addr}]
    assert :ok == StaticConfig.add(configs, accountA, shardA)
    assert :ok == StaticConfig.add(configs, accountB, shardB)
    {:ok, consensus1} = Consensus.start_link(node_id: node1_id, node_addr: node1_addr, ledgers: ledgers1, config_lookup: configs, netmod: Fake, net: net, protocol: ZftLock)
    tx1 = Transaction.new(accountA, accountB, 100, 0)
    Consensus.process_tx(consensus1, {tx1, nil})
    #zft_state = instance_state(consensus1, accountA)
    #assert zft_state.tx_queue == []
    #proposal = zft_state.voted_for
    #assert proposal.body.tx == tx1
    :timer.sleep(200)
    assert Ledger.account(ledger1A).balance == -100 + @start_balance
    assert Ledger.account(ledger1B).balance == 100 + @start_balance
  end

  test "single node, two rounds, no queue, same sender", context do
    %{
      accountA: accountA, accountB: accountB,
      node1_id: node1_id,
      node1_addr: node1_addr,
      ledgers1: ledgers1,
      configs: configs, net: net,
    } = context
    # Set up accounts and shards
    ledger1A = Ledgers.create(ledgers1, accountA)
    ledger1B = Ledgers.create(ledgers1, accountB)
    shardA = [{node1_id, node1_addr}]
    shardB = [{node1_id, node1_addr}]
    assert :ok == StaticConfig.add(configs, accountA, shardA)
    assert :ok == StaticConfig.add(configs, accountB, shardB)
    {:ok, consensus1} = Consensus.start_link(node_id: node1_id, node_addr: node1_addr, ledgers: ledgers1, config_lookup: configs, netmod: Fake, net: net, protocol: ZftLock)
    # First TX
    tx1 = Transaction.new(accountA, accountB, 100, 0)
    Consensus.process_tx(consensus1, {tx1, nil})
    :timer.sleep(100)
    assert Ledger.account(ledger1A).balance == -100 + @start_balance
    assert Ledger.account(ledger1B).balance == 100 + @start_balance
    # Second TX
    tx2 = Transaction.new(accountA, accountB, 100, 1)
    Consensus.process_tx(consensus1, {tx2, nil})
    :timer.sleep(100)
    assert Ledger.account(ledger1A).balance == -200 + @start_balance
    assert Ledger.account(ledger1B).balance == 200 + @start_balance
  end

  test "single node, two enqueued TXs, same sender", context do
    %{
      accountA: accountA, accountB: accountB,
      node1_id: node1_id,
      node1_addr: node1_addr,
      ledgers1: ledgers1,
      configs: configs, net: net,
    } = context
    # Set up accounts and shards
    ledger1A = Ledgers.create(ledgers1, accountA)
    ledger1B = Ledgers.create(ledgers1, accountB)
    shardA = [{node1_id, node1_addr}]
    shardB = [{node1_id, node1_addr}]
    assert :ok == StaticConfig.add(configs, accountA, shardA)
    assert :ok == StaticConfig.add(configs, accountB, shardB)
    {:ok, consensus1} = Consensus.start_link(node_id: node1_id, node_addr: node1_addr, ledgers: ledgers1, config_lookup: configs, netmod: Fake, net: net, protocol: ZftLock)
    # Both TXs
    tx1 = Transaction.new(accountA, accountB, 100, 0)
    tx2 = Transaction.new(accountA, accountB, 100, 1)
    Consensus.process_tx(consensus1, {tx1, nil})
    Consensus.process_tx(consensus1, {tx2, nil})
    :timer.sleep(200)
    assert Ledger.account(ledger1A).balance == -200 + @start_balance
    assert Ledger.account(ledger1B).balance == 200 + @start_balance
  end

  # FIXME: It is possible for one (probably the second) TX to fail, but
  # that is not supposed to happen! There is no good reason for that!
  test "two rounds, same sender", context do
    %{
      accountA: accountA, accountB: accountB,
      node1_id: node1_id, node2_id: node2_id, node3_id: node3_id,
      node1_addr: node1_addr, node2_addr: node2_addr, node3_addr: node3_addr,
      ledgers1: ledgers1,
      ledgers2: ledgers2,
      ledgers3: ledgers3,
      configs: configs, net: net,
    } = context
    ledgerA1 = Ledgers.create(ledgers1, accountA)
    ledgerA2 = Ledgers.create(ledgers2, accountA)
    ledgerB2 = Ledgers.create(ledgers2, accountB)
    ledgerB3 = Ledgers.create(ledgers3, accountB)
    # Consensus
    {:ok, consensus1} = Consensus.start_link(node_id: node1_id, node_addr: node1_addr, ledgers: ledgers1, config_lookup: configs, netmod: Fake, net: net, protocol: ZftLock)
    {:ok, _consensus2} = Consensus.start_link(node_id: node2_id, node_addr: node2_addr, ledgers: ledgers2, config_lookup: configs, netmod: Fake, net: net, protocol: ZftLock)
    {:ok, _consensus3} = Consensus.start_link(node_id: node3_id, node_addr: node3_addr, ledgers: ledgers3, config_lookup: configs, netmod: Fake, net: net, protocol: ZftLock)
    # Define shard configs
    shardA = [{node1_id, node1_addr}, {node2_id, node2_addr}]
    shardB = [{node2_id, node2_addr}, {node3_id, node3_addr}]
    assert :ok == StaticConfig.add(configs, accountA, shardA)
    assert :ok == StaticConfig.add(configs, accountB, shardB)
    # Create two transactions
    tx1 = Transaction.new(accountA, accountB, 100, 0)
    tx2 = Transaction.new(accountA, accountB, 50, 1)
    # Submit both TX
    Consensus.process_tx(consensus1, {tx1, nil})
    Consensus.process_tx(consensus1, {tx2, nil})
    # Expecting the two TXs to be appropriately serialized.
    # (The second must be enqueued and then processed after the first is done.)
    Process.sleep(200)
    # TODO: Assert something about the ledger itself! (Not just the account)
    # Ledger 1
    assert Ledger.account(ledgerA1).balance == -150 + @start_balance
    # Ledger 2
    assert Ledger.account(ledgerA2).balance == -150 + @start_balance
    assert Ledger.account(ledgerB2).balance == 150 + @start_balance
    # Ledger 3
    assert Ledger.account(ledgerB3).balance == 150 + @start_balance
  end

  test "two rounds, different sender, both succeed", context do
    %{
      accountA: accountA, accountB: accountB,
      node1_id: node1_id, node2_id: node2_id, node3_id: node3_id,
      node1_addr: node1_addr, node2_addr: node2_addr, node3_addr: node3_addr,
      ledgers1: ledgers1,
      ledgers2: ledgers2,
      ledgers3: ledgers3,
      configs: configs, net: net,
    } = context
    node4_id = Crypto.sha256(444)
    node4_addr = {{127, 0, 0, 4}, 10_004}
    {:ok, ledgers4} = Ledgers.start_link([])
    ledgerA1 = Ledgers.create(ledgers1, accountA)
    ledgerA2 = Ledgers.create(ledgers2, accountA)
    ledgerB2 = Ledgers.create(ledgers2, accountB)
    ledgerA3 = Ledgers.create(ledgers3, accountA)
    ledgerB3 = Ledgers.create(ledgers3, accountB)
    ledgerB4 = Ledgers.create(ledgers4, accountB)
    # Consensus
    {:ok, consensus1} = Consensus.start_link(node_id: node1_id, node_addr: node1_addr, ledgers: ledgers1, config_lookup: configs, netmod: Fake, net: net, protocol: ZftLock)
    {:ok, consensus2} = Consensus.start_link(node_id: node2_id, node_addr: node2_addr, ledgers: ledgers2, config_lookup: configs, netmod: Fake, net: net, protocol: ZftLock)
    {:ok, _consensus3} = Consensus.start_link(node_id: node3_id, node_addr: node3_addr, ledgers: ledgers3, config_lookup: configs, netmod: Fake, net: net, protocol: ZftLock)
    {:ok, _consensus4} = Consensus.start_link(node_id: node4_id, node_addr: node4_addr, ledgers: ledgers4, config_lookup: configs, netmod: Fake, net: net, protocol: ZftLock)
    # Define shard configs
    shardA = [{node1_id, node1_addr}, {node2_id, node2_addr}, {node3_id, node3_addr}]
    shardB = [{node2_id, node2_addr}, {node3_id, node3_addr}, {node4_id, node4_addr}]
    assert :ok == StaticConfig.add(configs, accountA, shardA)
    assert :ok == StaticConfig.add(configs, accountB, shardB)
    # Create two transactions
    tx1 = Transaction.new(accountA, accountB, 100, 0)
    tx2 = Transaction.new(accountB, accountA, 50, 0)
    # Submit both TXs
    Consensus.process_tx(consensus1, {tx1, nil})
    Consensus.process_tx(consensus2, {tx2, nil})
    Process.sleep(200)
    # TODO: Assert something about the ledger itself! (Not just the account)
    # Ledger 1
    assert Ledger.account(ledgerA1).balance == -50 + @start_balance
    # Ledger 2
    assert Ledger.account(ledgerA2).balance == -50 + @start_balance
    assert Ledger.account(ledgerB2).balance == 50 + @start_balance
    # Ledger 3
    assert Ledger.account(ledgerA3).balance == -50 + @start_balance
    assert Ledger.account(ledgerB3).balance == 50 + @start_balance
    # Ledger 4
    assert Ledger.account(ledgerB4).balance == 50 + @start_balance
  end

  test "three rounds, different senders, all three succeed", context do
    %{
      accountA: accountA, accountB: accountB,
      node1_id: node1_id, node2_id: node2_id, node3_id: node3_id,
      node1_addr: node1_addr, node2_addr: node2_addr, node3_addr: node3_addr,
      ledgers1: ledgers1,
      ledgers2: ledgers2,
      ledgers3: ledgers3,
      configs: configs, net: net,
    } = context
    accountC = Crypto.sha256(345)
    node4_id = Crypto.sha256(444)
    node4_addr = {{127, 0, 0, 4}, 10_004}
    {:ok, ledgers4} = Ledgers.start_link([])
    ledgerA1 = Ledgers.create(ledgers1, accountA)
    ledgerA2 = Ledgers.create(ledgers2, accountA)
    ledgerB2 = Ledgers.create(ledgers2, accountB)
    ledgerB3 = Ledgers.create(ledgers3, accountB)
    ledgerC3 = Ledgers.create(ledgers3, accountC)
    ledgerC4 = Ledgers.create(ledgers4, accountC)
    # Consensus
    {:ok, consensus1} = Consensus.start_link(node_id: node1_id, node_addr: node1_addr, ledgers: ledgers1, config_lookup: configs, netmod: Fake, net: net, protocol: ZftLock)
    {:ok, consensus2} = Consensus.start_link(node_id: node2_id, node_addr: node2_addr, ledgers: ledgers2, config_lookup: configs, netmod: Fake, net: net, protocol: ZftLock)
    {:ok, consensus3} = Consensus.start_link(node_id: node3_id, node_addr: node3_addr, ledgers: ledgers3, config_lookup: configs, netmod: Fake, net: net, protocol: ZftLock)
    {:ok, _consensus4} = Consensus.start_link(node_id: node4_id, node_addr: node4_addr, ledgers: ledgers4, config_lookup: configs, netmod: Fake, net: net, protocol: ZftLock)
    # Define shard configs
    shardA = [{node1_id, node1_addr}, {node2_id, node2_addr}]
    shardB = [{node2_id, node2_addr}, {node3_id, node3_addr}]
    shardC = [{node3_id, node3_addr}, {node4_id, node4_addr}]
    assert :ok == StaticConfig.add(configs, accountA, shardA)
    assert :ok == StaticConfig.add(configs, accountB, shardB)
    assert :ok == StaticConfig.add(configs, accountC, shardC)
    # Create two transactions
    tx1 = Transaction.new(accountA, accountB, 100, 0)
    tx2 = Transaction.new(accountB, accountA, 50, 0)
    tx3 = Transaction.new(accountC, accountA, 10, 0)
    # Submit both TXs
    Consensus.process_tx(consensus1, {tx1, nil})
    Consensus.process_tx(consensus2, {tx2, nil})
    Consensus.process_tx(consensus3, {tx3, nil})
    Process.sleep(200)
    # TODO: Assert something about the ledger itself! (Not just the account)
    # Ledger 1
    assert Ledger.account(ledgerA1).balance == -40 + @start_balance
    # Ledger 2
    assert Ledger.account(ledgerA2).balance == -40 + @start_balance
    assert Ledger.account(ledgerB2).balance == 50 + @start_balance
    # Ledger 3
    assert Ledger.account(ledgerB3).balance == 50 + @start_balance
    assert Ledger.account(ledgerC3).balance == -10 + @start_balance
    # Ledger 4
    assert Ledger.account(ledgerC4).balance == -10 + @start_balance
  end

  test "one to many", context do
    %{
      accountA: accountA, accountB: accountB,
      node1_id: node1_id, node2_id: node2_id, node3_id: node3_id,
      node1_addr: node1_addr, node2_addr: node2_addr, node3_addr: node3_addr,
      ledgers1: ledgers1,
      ledgers2: ledgers2,
      ledgers3: ledgers3,
      configs: configs, net: net,
    } = context
    accountC = Crypto.sha256(345)
    node4_id = Crypto.sha256(444)
    node4_addr = {{127, 0, 0, 4}, 10_004}
    {:ok, ledgers4} = Ledgers.start_link([])
    ledgerA1 = Ledgers.create(ledgers1, accountA)
    ledgerA2 = Ledgers.create(ledgers2, accountA)
    ledgerB2 = Ledgers.create(ledgers2, accountB)
    ledgerB3 = Ledgers.create(ledgers3, accountB)
    ledgerC3 = Ledgers.create(ledgers3, accountC)
    ledgerC4 = Ledgers.create(ledgers4, accountC)
    # Consensus
    {:ok, consensus1} = Consensus.start_link(node_id: node1_id, node_addr: node1_addr, ledgers: ledgers1, config_lookup: configs, netmod: Fake, net: net, protocol: ZftLock)
    {:ok, _consensus2} = Consensus.start_link(node_id: node2_id, node_addr: node2_addr, ledgers: ledgers2, config_lookup: configs, netmod: Fake, net: net, protocol: ZftLock)
    {:ok, _consensus3} = Consensus.start_link(node_id: node3_id, node_addr: node3_addr, ledgers: ledgers3, config_lookup: configs, netmod: Fake, net: net, protocol: ZftLock)
    {:ok, _consensus4} = Consensus.start_link(node_id: node4_id, node_addr: node4_addr, ledgers: ledgers4, config_lookup: configs, netmod: Fake, net: net, protocol: ZftLock)
    # Define shard configs
    shardA = [{node1_id, node1_addr}, {node2_id, node2_addr}]
    shardB = [{node2_id, node2_addr}, {node3_id, node3_addr}]
    shardC = [{node3_id, node3_addr}, {node4_id, node4_addr}]
    assert :ok == StaticConfig.add(configs, accountA, shardA)
    assert :ok == StaticConfig.add(configs, accountB, shardB)
    assert :ok == StaticConfig.add(configs, accountC, shardC)
    # Create two transactions
    tx1 = Transaction.new(accountA, accountB, 100, 0)
    tx2 = Transaction.new(accountA, accountC, 150, 1)
    tx3 = Transaction.new(accountA, accountC, 110, 2)
    tx4 = Transaction.new(accountA, accountB, 120, 3)
    tx5 = Transaction.new(accountA, accountB, 115, 4)
    tx6 = Transaction.new(accountA, accountC, 200, 5)
    # Submit both TXs
    Consensus.process_tx(consensus1, {tx1, nil})
    Consensus.process_tx(consensus1, {tx2, nil})
    Consensus.process_tx(consensus1, {tx3, nil})
    Consensus.process_tx(consensus1, {tx4, nil})
    Consensus.process_tx(consensus1, {tx5, nil})
    Consensus.process_tx(consensus1, {tx6, nil})
    Process.sleep(200)
    # TODO: Assert something about the ledger itself! (Not just the account)
    # Ledger 1
    toB = (100+120+115)
    toC = (150+110+200)
    fromA = toB + toC
    assert Ledger.account(ledgerA1).balance == -fromA + @start_balance
    # Ledger 2
    assert Ledger.account(ledgerA2).balance == -fromA + @start_balance
    assert Ledger.account(ledgerB2).balance == toB + @start_balance
    # Ledger 3
    assert Ledger.account(ledgerB3).balance == toB + @start_balance
    assert Ledger.account(ledgerC3).balance == toC + @start_balance
    # Ledger 4
    assert Ledger.account(ledgerC4).balance == toC + @start_balance
  end

  test "many to one", context do
    %{
      accountA: accountA, accountB: accountB,
      node1_id: node1_id, node2_id: node2_id, node3_id: node3_id,
      node1_addr: node1_addr, node2_addr: node2_addr, node3_addr: node3_addr,
      ledgers1: ledgers1,
      ledgers2: ledgers2,
      ledgers3: ledgers3,
      configs: configs, net: net,
    } = context
    accountC = Crypto.sha256(345)
    node4_id = Crypto.sha256(444)
    node4_addr = {{127, 0, 0, 4}, 10_004}
    {:ok, ledgers4} = Ledgers.start_link([])
    ledgerA1 = Ledgers.create(ledgers1, accountA)
    ledgerA2 = Ledgers.create(ledgers2, accountA)
    ledgerB2 = Ledgers.create(ledgers2, accountB)
    ledgerB3 = Ledgers.create(ledgers3, accountB)
    ledgerC3 = Ledgers.create(ledgers3, accountC)
    ledgerC4 = Ledgers.create(ledgers4, accountC)
    # Consensus
    {:ok, _consensus1} = Consensus.start_link(node_id: node1_id, node_addr: node1_addr, ledgers: ledgers1, config_lookup: configs, netmod: Fake, net: net, protocol: ZftLock)
    {:ok, consensus2} = Consensus.start_link(node_id: node2_id, node_addr: node2_addr, ledgers: ledgers2, config_lookup: configs, netmod: Fake, net: net, protocol: ZftLock)
    {:ok, consensus3} = Consensus.start_link(node_id: node3_id, node_addr: node3_addr, ledgers: ledgers3, config_lookup: configs, netmod: Fake, net: net, protocol: ZftLock)
    {:ok, _consensus4} = Consensus.start_link(node_id: node4_id, node_addr: node4_addr, ledgers: ledgers4, config_lookup: configs, netmod: Fake, net: net, protocol: ZftLock)
    # Define shard configs
    shardA = [{node1_id, node1_addr}, {node2_id, node2_addr}]
    shardB = [{node2_id, node2_addr}, {node3_id, node3_addr}]
    shardC = [{node3_id, node3_addr}, {node4_id, node4_addr}]
    assert :ok == StaticConfig.add(configs, accountA, shardA)
    assert :ok == StaticConfig.add(configs, accountB, shardB)
    assert :ok == StaticConfig.add(configs, accountC, shardC)
    # Create two transactions
    tx1 = Transaction.new(accountB, accountA, 100, 0)
    tx2 = Transaction.new(accountC, accountA, 150, 0)
    tx3 = Transaction.new(accountB, accountA, 110, 1)
    tx4 = Transaction.new(accountB, accountA, 120, 2)
    tx5 = Transaction.new(accountC, accountA, 115, 1)
    tx6 = Transaction.new(accountC, accountA, 200, 2)
    # Submit both TXs
    Consensus.process_tx(consensus2, {tx1, nil})
    Consensus.process_tx(consensus3, {tx2, nil})
    Consensus.process_tx(consensus2, {tx3, nil})
    Consensus.process_tx(consensus2, {tx4, nil})
    Consensus.process_tx(consensus3, {tx5, nil})
    Consensus.process_tx(consensus3, {tx6, nil})
    Process.sleep(200)
    # TODO: Assert something about the ledger itself! (Not just the account)
    # Ledger 1
    fromB = (100+110+120)
    fromC = (150+115+200)
    toA = fromB + fromC
    assert Ledger.account(ledgerA1).balance == toA + @start_balance
    # Ledger 2
    assert Ledger.account(ledgerA2).balance == toA + @start_balance
    assert Ledger.account(ledgerB2).balance == -fromB + @start_balance
    # Ledger 3
    assert Ledger.account(ledgerB3).balance == -fromB + @start_balance
    assert Ledger.account(ledgerC3).balance == -fromC + @start_balance
    # Ledger 4
    assert Ledger.account(ledgerC4).balance == -fromC + @start_balance
  end

  test "one to many, jumbled nonce", context do
    %{
      accountA: accountA, accountB: accountB,
      node1_id: node1_id, node2_id: node2_id, node3_id: node3_id,
      node1_addr: node1_addr, node2_addr: node2_addr, node3_addr: node3_addr,
      ledgers1: ledgers1,
      ledgers2: ledgers2,
      ledgers3: ledgers3,
      configs: configs, net: net,
    } = context
    accountC = Crypto.sha256(345)
    node4_id = Crypto.sha256(444)
    node4_addr = {{127, 0, 0, 4}, 10_004}
    {:ok, ledgers4} = Ledgers.start_link([])
    ledgerA1 = Ledgers.create(ledgers1, accountA)
    ledgerA2 = Ledgers.create(ledgers2, accountA)
    ledgerB2 = Ledgers.create(ledgers2, accountB)
    ledgerB3 = Ledgers.create(ledgers3, accountB)
    ledgerC3 = Ledgers.create(ledgers3, accountC)
    ledgerC4 = Ledgers.create(ledgers4, accountC)
    # Consensus
    {:ok, consensus1} = Consensus.start_link(node_id: node1_id, node_addr: node1_addr, ledgers: ledgers1, config_lookup: configs, netmod: Fake, net: net, protocol: ZftLock)
    {:ok, _consensus2} = Consensus.start_link(node_id: node2_id, node_addr: node2_addr, ledgers: ledgers2, config_lookup: configs, netmod: Fake, net: net, protocol: ZftLock)
    {:ok, _consensus3} = Consensus.start_link(node_id: node3_id, node_addr: node3_addr, ledgers: ledgers3, config_lookup: configs, netmod: Fake, net: net, protocol: ZftLock)
    {:ok, _consensus4} = Consensus.start_link(node_id: node4_id, node_addr: node4_addr, ledgers: ledgers4, config_lookup: configs, netmod: Fake, net: net, protocol: ZftLock)
    # Define shard configs
    shardA = [{node1_id, node1_addr}, {node2_id, node2_addr}]
    shardB = [{node2_id, node2_addr}, {node3_id, node3_addr}]
    shardC = [{node3_id, node3_addr}, {node4_id, node4_addr}]
    assert :ok == StaticConfig.add(configs, accountA, shardA)
    assert :ok == StaticConfig.add(configs, accountB, shardB)
    assert :ok == StaticConfig.add(configs, accountC, shardC)
    # Create two transactions
    tx1 = Transaction.new(accountA, accountB, 100, 3)
    tx2 = Transaction.new(accountA, accountC, 150, 2)
    tx3 = Transaction.new(accountA, accountC, 110, 4)
    tx4 = Transaction.new(accountA, accountB, 120, 0)
    tx5 = Transaction.new(accountA, accountB, 115, 5)
    tx6 = Transaction.new(accountA, accountC, 200, 1)
    # Submit both TXs
    Consensus.process_tx(consensus1, {tx1, nil})
    Consensus.process_tx(consensus1, {tx2, nil})
    Consensus.process_tx(consensus1, {tx3, nil})
    Consensus.process_tx(consensus1, {tx4, nil})
    Consensus.process_tx(consensus1, {tx5, nil})
    Consensus.process_tx(consensus1, {tx6, nil})
    Process.sleep(200)
    # TODO: Assert something about the ledger itself! (Not just the account)
    # Ledger 1
    toB = (100+120+115)
    toC = (150+110+200)
    fromA = toB + toC
    assert Ledger.account(ledgerA1).balance == -fromA + @start_balance
    # Ledger 2
    assert Ledger.account(ledgerA2).balance == -fromA + @start_balance
    assert Ledger.account(ledgerB2).balance == toB + @start_balance
    # Ledger 3
    assert Ledger.account(ledgerB3).balance == toB + @start_balance
    assert Ledger.account(ledgerC3).balance == toC + @start_balance
    # Ledger 4
    assert Ledger.account(ledgerC4).balance == toC + @start_balance
  end

  test "many to one, jumbled nonce", context do
    %{
      accountA: accountA, accountB: accountB,
      node1_id: node1_id, node2_id: node2_id, node3_id: node3_id,
      node1_addr: node1_addr, node2_addr: node2_addr, node3_addr: node3_addr,
      ledgers1: ledgers1,
      ledgers2: ledgers2,
      ledgers3: ledgers3,
      configs: configs, net: net,
    } = context
    accountC = Crypto.sha256(345)
    node4_id = Crypto.sha256(444)
    node4_addr = {{127, 0, 0, 4}, 10_004}
    {:ok, ledgers4} = Ledgers.start_link([])
    ledgerA1 = Ledgers.create(ledgers1, accountA)
    ledgerA2 = Ledgers.create(ledgers2, accountA)
    ledgerB2 = Ledgers.create(ledgers2, accountB)
    ledgerB3 = Ledgers.create(ledgers3, accountB)
    ledgerC3 = Ledgers.create(ledgers3, accountC)
    ledgerC4 = Ledgers.create(ledgers4, accountC)
    # Consensus
    {:ok, _consensus1} = Consensus.start_link(node_id: node1_id, node_addr: node1_addr, ledgers: ledgers1, config_lookup: configs, netmod: Fake, net: net, protocol: ZftLock)
    {:ok, consensus2} = Consensus.start_link(node_id: node2_id, node_addr: node2_addr, ledgers: ledgers2, config_lookup: configs, netmod: Fake, net: net, protocol: ZftLock)
    {:ok, consensus3} = Consensus.start_link(node_id: node3_id, node_addr: node3_addr, ledgers: ledgers3, config_lookup: configs, netmod: Fake, net: net, protocol: ZftLock)
    {:ok, _consensus4} = Consensus.start_link(node_id: node4_id, node_addr: node4_addr, ledgers: ledgers4, config_lookup: configs, netmod: Fake, net: net, protocol: ZftLock)
    # Define shard configs
    shardA = [{node1_id, node1_addr}, {node2_id, node2_addr}]
    shardB = [{node2_id, node2_addr}, {node3_id, node3_addr}]
    shardC = [{node3_id, node3_addr}, {node4_id, node4_addr}]
    assert :ok == StaticConfig.add(configs, accountA, shardA)
    assert :ok == StaticConfig.add(configs, accountB, shardB)
    assert :ok == StaticConfig.add(configs, accountC, shardC)
    # Create two transactions
    tx1 = Transaction.new(accountB, accountA, 100, 1)
    tx2 = Transaction.new(accountC, accountA, 150, 0)
    tx3 = Transaction.new(accountB, accountA, 110, 2)
    tx4 = Transaction.new(accountB, accountA, 120, 0)
    tx5 = Transaction.new(accountC, accountA, 115, 2)
    tx6 = Transaction.new(accountC, accountA, 200, 1)
    # Submit both TXs
    Consensus.process_tx(consensus2, {tx1, nil})
    Consensus.process_tx(consensus3, {tx2, nil})
    Consensus.process_tx(consensus2, {tx3, nil})
    Consensus.process_tx(consensus2, {tx4, nil})
    Consensus.process_tx(consensus3, {tx5, nil})
    Consensus.process_tx(consensus3, {tx6, nil})
    Process.sleep(200)
    # TODO: Assert something about the ledger itself! (Not just the account)
    # Ledger 1
    fromB = (100+110+120)
    fromC = (150+115+200)
    toA = fromB + fromC
    assert Ledger.account(ledgerA1).balance == toA + @start_balance
    # Ledger 2
    assert Ledger.account(ledgerA2).balance == toA + @start_balance
    assert Ledger.account(ledgerB2).balance == -fromB + @start_balance
    # Ledger 3
    assert Ledger.account(ledgerB3).balance == -fromB + @start_balance
    assert Ledger.account(ledgerC3).balance == -fromC + @start_balance
    # Ledger 4
    assert Ledger.account(ledgerC4).balance == -fromC + @start_balance
  end

  test "TXs that exceed balance are ignored, simple", context do
    %{
      accountA: accountA, accountB: accountB,
      node1_id: node1_id, node2_id: node2_id, node3_id: node3_id,
      node1_addr: node1_addr, node2_addr: node2_addr, node3_addr: node3_addr,
      ledgers1: ledgers1,
      ledgers2: ledgers2,
      ledgers3: ledgers3,
      configs: configs, net: net,
    } = context
    ledgerA1 = Ledgers.create(ledgers1, accountA)
    ledgerA2 = Ledgers.create(ledgers2, accountA)
    ledgerB2 = Ledgers.create(ledgers2, accountB)
    ledgerB3 = Ledgers.create(ledgers3, accountB)
    # Consensus
    {:ok, consensus1} = Consensus.start_link(node_id: node1_id, node_addr: node1_addr, ledgers: ledgers1, config_lookup: configs, netmod: Fake, net: net, protocol: ZftLock)
    {:ok, _consensus2} = Consensus.start_link(node_id: node2_id, node_addr: node2_addr, ledgers: ledgers2, config_lookup: configs, netmod: Fake, net: net, protocol: ZftLock)
    {:ok, _consensus3} = Consensus.start_link(node_id: node3_id, node_addr: node3_addr, ledgers: ledgers3, config_lookup: configs, netmod: Fake, net: net, protocol: ZftLock)
    # Define shard configs
    shardA = [{node1_id, node1_addr}, {node2_id, node2_addr}]
    shardB = [{node2_id, node2_addr}, {node3_id, node3_addr}]
    assert :ok == StaticConfig.add(configs, accountA, shardA)
    assert :ok == StaticConfig.add(configs, accountB, shardB)
    # Create two transactions
    tx0 = Transaction.new(accountA, accountB, @start_balance+1, 0)  # ignored
    tx1 = Transaction.new(accountA, accountB, @start_balance-1, 0)  # A=1     B=1999
    tx2 = Transaction.new(accountA, accountB, 2, 1)                 # ignored
    tx3 = Transaction.new(accountA, accountB, 1, 1)                 # A=0     B=2000
    # Submit both TXs
    Consensus.process_tx(consensus1, {tx0, nil})
    Consensus.process_tx(consensus1, {tx1, nil})
    Consensus.process_tx(consensus1, {tx2, nil})
    Consensus.process_tx(consensus1, {tx3, nil})
    Process.sleep(200)
    # TODO: Assert something about the ledger itself! (Not just the account)
    # Ledger 1
    assert Ledger.account(ledgerA1).balance == 0
    # Ledger 2
    assert Ledger.account(ledgerA2).balance == 0
    assert Ledger.account(ledgerB2).balance == 2*@start_balance
    # Ledger 3
    assert Ledger.account(ledgerB3).balance == 2*@start_balance
  end

  test "TXs that exceed balance are ignored, medium", context do
    %{
      accountA: accountA, accountB: accountB,
      node1_id: node1_id, node2_id: node2_id, node3_id: node3_id,
      node1_addr: node1_addr, node2_addr: node2_addr, node3_addr: node3_addr,
      ledgers1: ledgers1,
      ledgers2: ledgers2,
      ledgers3: ledgers3,
      configs: configs, net: net,
    } = context
    ledgerA1 = Ledgers.create(ledgers1, accountA)
    ledgerA2 = Ledgers.create(ledgers2, accountA)
    ledgerB2 = Ledgers.create(ledgers2, accountB)
    ledgerB3 = Ledgers.create(ledgers3, accountB)
    # Consensus
    {:ok, consensus1} = Consensus.start_link(node_id: node1_id, node_addr: node1_addr, ledgers: ledgers1, config_lookup: configs, netmod: Fake, net: net, protocol: ZftLock)
    {:ok, consensus2} = Consensus.start_link(node_id: node2_id, node_addr: node2_addr, ledgers: ledgers2, config_lookup: configs, netmod: Fake, net: net, protocol: ZftLock)
    {:ok, _consensus3} = Consensus.start_link(node_id: node3_id, node_addr: node3_addr, ledgers: ledgers3, config_lookup: configs, netmod: Fake, net: net, protocol: ZftLock)
    # Define shard configs
    shardA = [{node1_id, node1_addr}, {node2_id, node2_addr}]
    shardB = [{node2_id, node2_addr}, {node3_id, node3_addr}]
    assert :ok == StaticConfig.add(configs, accountA, shardA)
    assert :ok == StaticConfig.add(configs, accountB, shardB)
    # Create two transactions
    tx0 = Transaction.new(accountA, accountB, @start_balance+1, 0)    # ignored
    tx1 = Transaction.new(accountA, accountB, @start_balance-1, 0)    # A=1     B=1999
    tx2 = Transaction.new(accountA, accountB, 3, 1)                   # ignored
    tx3 = Transaction.new(accountA, accountB, 1, 1)                   # A=0     B=2000
    tx4 = Transaction.new(accountB, accountA, 1, 0)                   # A=1     B=1999
    tx5 = Transaction.new(accountB, accountA, 2*@start_balance+1, 1)  # ignored
    tx6 = Transaction.new(accountB, accountA, @start_balance, 1)      # A=1001  B=999
    # Submit both TXs
    Consensus.process_tx(consensus1, {tx0, nil})
    Consensus.process_tx(consensus1, {tx1, nil})
    Consensus.process_tx(consensus1, {tx2, nil})
    Consensus.process_tx(consensus1, {tx3, nil})
    Consensus.process_tx(consensus2, {tx4, nil})
    Consensus.process_tx(consensus2, {tx5, nil})
    Consensus.process_tx(consensus2, {tx6, nil})
    Process.sleep(200)
    # TODO: Assert something about the ledger itself! (Not just the account)
    # Ledger 1
    assert Ledger.account(ledgerA1).balance == @start_balance + 1
    # Ledger 2
    assert Ledger.account(ledgerA2).balance == @start_balance + 1
    assert Ledger.account(ledgerB2).balance == @start_balance - 1
    # Ledger 3
    assert Ledger.account(ledgerB3).balance == @start_balance - 1
  end

end
