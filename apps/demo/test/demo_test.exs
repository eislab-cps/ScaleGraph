defmodule DemoTest do
  use ExUnit.Case
  #doctest Demo

  setup(_context) do
    Demo.reset()
  end

  test "reset stops the server and starts a new server" do
    pid1 = check_running()
    Demo.reset()
    pid2 = check_running()
    assert pid1 != pid2
    # Old server was stopped.
    assert not Process.alive?(pid1)
  end

  test "reset starts a new server if not running" do
    stop()
    check_not_running()
    Demo.reset()
    check_running()
  end

  test "reset setting id_bits and shards_size" do
    Demo.reset(id_bits: 45, shard_size: 5)
    state = state()
    assert state.id_bits == 45
    assert state.shard_size == 5
  end

  test "setup starts a new server if not running" do
    stop()
    check_not_running()
    Demo.setup([:noshow])
    # Is now running.
    check_running()
  end

  test "setup sets id_bits and shard_size if not running" do
    stop()
    check_not_running()
    Demo.setup([:noshow, id_bits: 48, shard_size: 13])
    check_running()
    state = state()
    assert state.id_bits == 48
    assert state.shard_size == 13
    assert map_size(state.nodes_by_index) == 2*13
    assert map_size(state.clients_by_index) == 13
  end

  test "setup uses shard_size if already running" do
    Demo.reset()
    state = state()
    shard_size = state.shard_size
    id_bits = state.id_bits
    Demo.setup([:noshow, id_bits: id_bits + 48, shard_size: shard_size + 13])
    check_running()
    state = state()
    assert state.shard_size == shard_size  # unchanged
    assert state.id_bits == id_bits  # unchanged
    # But shard size was used when adding nodes/clients
    assert map_size(state.nodes_by_index) == 2*(shard_size + 13)
    assert map_size(state.clients_by_index) == shard_size + 13
  end

  test "setup does not start a new server if already running" do
    pid1 = check_running()
    Demo.setup(noshow: true)
    pid2 = check_running()
    # Same server is running.
    assert pid1 == pid2
  end

  test "setup adds nodes and clients" do
    stop()
    check_not_running()
    # Setup
    Demo.setup([:noshow, nodes: 7, clients: 8])
    state = :sys.get_state(Demo)
    assert map_size(state.nodes_by_index) == 7
    assert map_size(state.clients_by_index) == 8
  end

  test "setup adds nodes and clients even if running" do
    state = :sys.get_state(Demo)
    nodes_before = map_size(state.nodes_by_index)
    clients_before = map_size(state.clients_by_index)
    # Setup
    Demo.setup([:noshow, nodes: 7, accounts: 8])
    state = :sys.get_state(Demo)
    assert map_size(state.nodes_by_index) == 7 + nodes_before
    assert map_size(state.clients_by_index) == 8 + clients_before
    Demo.setup([:noshow, nodes: 4, clients: 2])
    state = :sys.get_state(Demo)
    assert map_size(state.nodes_by_index) == 7 + 4 + nodes_before
    assert map_size(state.clients_by_index) == 8 + 2 + clients_before
  end

  test "add node with (hashed) ID" do
    Demo.add_node(id: 123)
    state = :sys.get_state(Demo)
    node = state.nodes_by_index[0]
    hashed = Crypto.sha256(:erlang.term_to_binary(123))
    <<expected::size(state.id_bits)-bitstring, _::bitstring>> = hashed
    assert node.node_id == expected
  end

  test "add node with (raw) ID" do
    Demo.add_node(id: 123, nohash: true)
    state = :sys.get_state(Demo)
    node = state.nodes_by_index[0]
    expected = <<123::size(state.id_bits)>>
    assert node.node_id == expected
  end

  defp check_not_running() do
    pid = GenServer.whereis(Demo)
    assert is_nil(pid) || not Process.alive?(pid)
  end

  defp check_running() do
    pid = GenServer.whereis(Demo)
    assert not is_nil(pid)
    assert Process.alive?(pid)
    pid
  end

  defp stop() do
    GenServer.stop(Demo, :normal)
  end

  defp state() do
    :sys.get_state(Demo)
  end

end
