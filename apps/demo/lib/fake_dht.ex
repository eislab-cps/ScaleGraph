defmodule FakeDht do
  @moduledoc """
  Shard config lookup based on fake DHT node lookup.

  Maintains a list of nodes and computes the `n` closest nodes as the shard.
  Note that adding shard configs is discouraged. Add individual nodes instead.
  """
  use GenServer

  def start_link(opts) do
    {nodes, opts} = Keyword.pop(opts, :nodes, [])
    {shard_size, opts} = Keyword.pop!(opts, :shard_size)
    GenServer.start_link(__MODULE__, [nodes: nodes, shard_size: shard_size], opts)
  end

  def add_node(dht, {_id, _addr}=node) do
    GenServer.call(dht, {:add_node, node})
  end

  @impl GenServer
  def init(opts) do
    state = %{
      nodes: Keyword.fetch!(opts, :nodes),
      shard_size: Keyword.fetch!(opts, :shard_size),
      shards: %{},
    }
    {:ok, state}
  end

  @impl GenServer
  def handle_call({:add_node, id_addr}, _caller, state) do
    # TODO: make sure nodes are unique (no duplicates)
    state = %{state |
      nodes: [id_addr | state.nodes],
      shards: %{},  # Must reset cache!
    }
    #state = Map.put(state, :nodes, [id_addr | state.nodes])
    {:reply, :ok, state}
  end

  @impl GenServer
  def handle_call({:add_config, account_id, nodes}, _caller, state) do
    # TODO: make sure nodes are unique (no duplicates)
    # TODO: What if we add the nodes, and the resulting shard is different
    #   from this given account ID -> [nodes] mapping?
    state = Map.put(state, :nodes, nodes ++ state.nodes)
    state = put_in(state.shards[account_id], nodes)
    {:reply, :ok, state}
  end

  @impl GenServer
  def handle_call({:get_config, account_id}, _caller, state) do
    case state.shards[account_id] do
      nil ->
        shard = state.nodes
          |> Enum.sort_by(fn {id, _addr} -> Util.distance(account_id, id) end)
          |> Enum.take(state.shard_size)
        state = put_in(state.shards[account_id], shard)
        {:reply, shard, state}
      shard ->
        {:reply, shard, state}
    end
  end

end
