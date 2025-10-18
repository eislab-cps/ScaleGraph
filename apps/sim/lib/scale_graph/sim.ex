# TODO:
# - Accept keys and/or IDs and/or addresses, generate only those that are
#   necessary (missing).
# - Check that node IDs and/or addresses are unique.
# - Dynamic node supervisor probably needs an adjustable max restart setting.
# - Collect all node addresses and names so that they can (optionally?) be
#   specified as a `:connect` option to Netsim.Fake and the network can
#   (optionally?) be restarted.
# - Currently assumes that a Fake network should be used. But we might also
#   want to run many nodes using UDP. So the network should be configurable.
# - Change how keys, id, and addr are passed to NodeSupervisor. They should
#   (probably!?) be given once as options directly to NodeSupervisor, rather
#   than repeated for Node, RPC, and any other component that might need one
#   or more of them. (NodeSupervisor can pass them along as needed.)
# - Currently, NodeSupervisors are always restarted. That may not always be
#   desirable:
#   - Figure out what restart strategy makes sense for the dynamic supervisor.
#     Should nodes be restarted? We probably want to make it configurable...
#   - ...that would mean we need to either build the appropriate child spec
#     here (with the appropriate `:restart` setting, or override the default
#     child_spec/1 in NodeSupervisor.
# - Does the simulation need a name? Does the dynamic supervisor need a name?
# - Add operations for manipulating a running simulation:
#   - Get the names of nodes (e.g. by address)
#   - Get the state of nodes.
#   - Add new nodes.
#   - Crash/restart and/or permanently take down nodes.
#   - Inject transactions (maybe add accounts/clients to the simulation?)
#   - Inject arbitrary messages?
#
# If we allow nodes to be added/removed during the simulation, how hard do
# we work to keep the state in sync with the DynamicSupervisor?

# Note that there are two notions of "node". There is `ScaleGraph.Node`, which
# is a process with an actual node implementation. Then there is
# `ScaleGraph.NodeSupervisor`, which runs a `ScaleGraph.Node` together with the
# processes a `ScaleGraph.Node` depends on, such as `ScaleGraph.RPC`.
# In general, a `ScaleGraph.Node` will be of primary interest for the purpose
# of poking at nodes and making things happen or checking the states of nodes.
# The corresponding NodeSupervisor is really only relevant when crashing/killing
# nodes.
defmodule ScaleGraph.Sim do
  @moduledoc """
  Simulates ScaleGraph networks, e.g. for testing.

  This module makes it convenient to run many nodes inside a single VM.
  """
  use GenServer, restart: :temporary
  require Logger

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts)
  end

  @doc """
  Make all nodes join the network.
  """
  def join(sim, opts) do
    GenServer.call(sim, {:join, opts})
  end

  @doc """
  Add a new node to the simulation. Accepts options `:keys`, `:id`, and `addr`
  and generates missing values (all are optional).
  """
  @deprecated "NOT IMPLEMENTED"
  def add_node(_sim, _opts) do
    raise "FIXME: not implemented"
  end

  @doc """
  Simulate a crash (and restart) of the given `node` (name, id, or addr).
  """
  def crash_node(sim, node) do
    GenServer.call(sim, {:crash_node, node})
  end

  @doc """
  Simulate a permanent crash of the given `node` (name, id, or addr).
  The node will not restart.
  """
  def kill_node(sim, node) do
    GenServer.call(sim, {:kill_node, node})
  end

  # Well, it IS implemented, but we might not want to keep it around.
  def node_pid(sim, node) do
    GenServer.call(sim, {:node_pid, node})
  end

  def node_name(sim, node) do
    GenServer.call(sim, {:node_name, node})
  end

  @impl GenServer
  def init(opts) do
    sim_id = System.unique_integer()
    node_count = Keyword.get(opts, :node_count, 0)
    id_bits = Keyword.get(opts, :id_bits, 256)
    shard_size = Keyword.get(opts, :shard_size, 5)
    keys = Enum.map(1..node_count, fn _ ->
      Crypto.generate_keys()
    end)
    ids = Enum.map(keys, &(&1.pub))
    addrs = Enum.map(1..node_count, fn i ->
      {{127, 54, 54, i}, 54_321}
    end)

    network_name = _network_via(sim_id)
    dynsuper_name = _dynsuper_via(sim_id)
    {:ok, dynsup} = DynamicSupervisor.start_link([name: dynsuper_name, strategy: :one_for_one])

    network = {Netsim.Fake, [name: network_name]}
    DynamicSupervisor.start_child(dynsup, network)

    node_names = Enum.map(addrs, &(_node_via(sim_id, &1)))

    node_specs =
      Enum.zip([keys, ids, addrs])
      |> Enum.map(fn {keys, id, addr} ->
        rpc_name = _rpc_via(sim_id, addr)
        rt_name = _rt_via(sim_id, addr)
        dht_name = _dht_via(sim_id, addr)
        node_name = _node_via(sim_id, addr)
        node_opts = [
          rpc: rpc_name,
          dht: dht_name,
          name: node_name,
        ]
        rpc_opts = [
          net: {Netsim.Fake, network_name},
          name: rpc_name,
        ]
        rt_opts = [
          id_bits: id_bits,
          bucket_size: shard_size,
          name: rt_name,
        ]
        dht_opts = [
          rpc: rpc_name,
          rt: rt_name,
          rt_mod: ScaleGraph.DHT.FakeRT,
          shard_size: shard_size,
          name: dht_name,
        ]
        node_sup_opts = [
          name: _node_sup_via(sim_id, addr),
          mode: :simulation,
          keys: keys,
          id: id,
          addr: addr,
          net_mod: Netsim.Fake,
          id_bits: id_bits,
          shard_size: shard_size,
          rpc_opts: rpc_opts,
          rt_opts: rt_opts,
          dht_opts: dht_opts,
          node_opts: node_opts,
          strategy: :one_for_one,
        ]
        # TODO: Make a child spec here so we can control restarts?
        # (Instead of relying on child_spec/1)
        child = {ScaleGraph.NodeSupervisor, node_sup_opts}
        {:ok, _} = DynamicSupervisor.start_child(dynsup, child)
        node_sup_opts
      end)

    node_supers = Enum.map(node_specs, &(&1[:name]))

    state = %{
      sim_id: sim_id,
      supervisor: dynsuper_name,
      network: network_name,
      netmod: Netsim.Fake,
      node_names: node_names,
      ids: ids,
      addrs: addrs,
      node_supers: node_supers,
    }
    {:ok, state}
  end

  # --- handlers ---

  @impl GenServer
  def handle_call({:join, _opts}, _caller, state) do
    [bootstrap | joining] = state.node_names
    bootstrap = :sys.get_state(bootstrap)
    bootstrap = {bootstrap.id, bootstrap.addr}
    Enum.each(joining, fn node ->
      result = ScaleGraph.Node.join(node, [bootstrap: [bootstrap]])
    end)
    {:reply, :ok, state}
  end

  # We need to find a NodeSupervisor using information about a Node.
  # TODO: Ideally, we would even allow a node name to be given, and then find
  # the corresponding node supervisor name/pid.
  @impl GenServer
  def handle_call({:kill_node, node}, _caller, state) do
    pid = _super_pid(state, node)
    dynsup = state.supervisor
    result = DynamicSupervisor.terminate_child(dynsup, pid)
    {:reply, result, state}
  end

  @impl GenServer
  def handle_call({:crash_node, node}, _caller, state) do
    pid = _super_pid(state, node)
    Process.exit(pid, :kill)
    {:reply, :ok, state}
  end

  @impl GenServer
  def handle_call({:node_name, node}, _caller, state) do
    result = _node_name(state, node)
    {:reply, result, state}
  end

  @impl GenServer
  def handle_call({:node_pid, node}, _caller, state) do
    pid = _node_pid(state, node)
    {:reply, pid, state}
  end

  defp _super_pid(_state, super) when is_pid(super), do: super

  defp _super_pid(state, super) do
    GenServer.whereis(_super_name(state, super))
  end

  defp _super_name(state, {_ip, _port} = addr) do
    Enum.find(state.node_supers, fn name ->
      addr == _addr_from_super_name(name)
    end)
  end

  # Should match whether we're given a Node name or a NodeSupervisor name.
  defp _super_name(_state, {:via, _, _} = _name) do
    raise "FIXME: not implemented"
  end

  defp _node_pid(_state, node) when is_pid(node), do: node

  defp _node_pid(state, node) do
    node_name = _node_name(state, node)
    GenServer.whereis(node_name)
  end

  defp _node_name(_state, id) when is_integer(id) do
    raise "FIXME: not implemented"
  end

  # TODO: Should we check that `name` exists in the simulation?
  defp _node_name(_state, {:via, _, _} = name), do: name

  # TODO: Should map ids and addrs to node names so we can look them up directly!
  defp _node_name(state, {_ip, _port} = addr) do
    Enum.find(state.node_names, fn name ->
      addr == _addr_from_node_name(name)
    end)
  end

  # Extract the address from a node name.
  defp _addr_from_node_name({:via, _reg, {_pid, {ScaleGraph.Node, addr, _sim_id}}}) do
    addr
  end

  # Extract the address from a node supervisor name.
  defp _addr_from_super_name({:via, _reg, {_pid, {ScaleGraph.NodeSupervisor, addr, _sim_id}}}) do
    addr
  end

  # --- Setup helpers ---

  defp _reg_name() do
    ScaleGraph.Sim.Application.regdir_name()
  end

  defp _network_via(sim_id) do
    {:via, Registry, {_reg_name(), {Network.Fake, sim_id}}}
  end

  # TODO: Do we even need a name!?
  #defp sim_name(sim_id) do
  #  {:via, Registry, {reg_name(), {__MODULE__, sim_id}}}
  #end

  defp _dynsuper_via(sim_id) do
    {:via, Registry, {_reg_name(), {DynamicNodeSupervisor, sim_id}}}
  end

  # TODO: Do we need/want to include the ID in the name?
  # The address is supposed to be unique already, and it is much more human
  # readable when printed (especially when using real 256-bit IDs).

  defp _node_via(sim_id, addr) do
    {:via, Registry, {_reg_name(), {ScaleGraph.Node, addr, sim_id}}}
  end

  defp _node_sup_via(sim_id, addr) do
    {:via, Registry, {_reg_name(), {ScaleGraph.NodeSupervisor, addr, sim_id}}}
  end

  defp _rpc_via(sim_id, addr) do
    {:via, Registry, {_reg_name(), {ScaleGraph.RPC, addr, sim_id}}}
  end

  defp _rt_via(sim_id, addr) do
    {:via, Registry, {_reg_name(), {ScaleGraph.RT, addr, sim_id}}}
  end

  defp _dht_via(sim_id, addr) do
    {:via, Registry, {_reg_name(), {ScaleGraph.DHT, addr, sim_id}}}
  end

end
