defmodule ScaleGraph.DHT do
  @moduledoc """
  ScaleGraph Kademlia-inspried DHT/XOR overlay.

  It does not store key-value pairs in the way DHTs (e.g. Kademlia) typically
  do. Primarily, it maps account IDs (key) to the ledger (value) for that
  account. However, the ledger is structured data, not an atomic blob, and peers
  would generally not request a whole ledger as a value, but rather certain
  blocks, especially the last block. The DHT also maps accounts to the list of
  nodes closest to that account.

  Another important difference is that a consensus protocol is used when adding
  transactions (blocks) to the ledger. (Strong consistency instead of eventual.)
  Nodes can not simply add values willy nilly as in Kademlia.

  We do not implement the classic Kademlia eviction policy. Instead of sorting
  buckets by recently seen (and evicting least recently seen dead nodes), we
  sort by distance and evict the most distant (dead) node.
  """
  use GenServer
  alias ScaleGraph.DHT.NodeLookup
  alias ScaleGraph.DHT.Contact

  defstruct [
    id: nil,
    rpc: nil,
    rt: nil,
    rt_mod: nil,
    shard_size: nil,
    # alpha, n, max_pool, etc.
    lookup_opts: nil,
  ]

  @doc """
  Start DHT as a linked process.

  Required options:
  - `:id` - this node's ID.
  - `:rpc` - the RPC server process (pid/atom/name).
  - `:rt_mod` - routing table module.
  - `:rt` - routing table process (pid/atom/name).
  - `:shard_size` - shard size.

  Optional:
  - `:bootstraps` - one or more nodes to use for bootstrapping.
  - `:join` - whether to automatically join the overlay network.
  - `:lookup_opts` - node lookup options (see `ScaleGraph.DHT.NodeLookup`).

  If no `:lookup_opts` is provided, or some options are missing, defaults are
  used for the following options:
  - `:rpc`
  - `:n_lookup` - set to `shard_size`
  """
  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, opts)
  end

  @doc """
  Join the overlay network.
  """
  def join(dht, opts \\ []) do
    GenServer.call(dht, {:join, opts})
  end

  @doc """
  Perform Kademlia-style node lookup to find the closest nodes to `id`.
  """
  def node_lookup(dht, id) do
    GenServer.call(dht, {:node_lookup, id})
  end

  @doc """
  Return the closest nodes to `id` from our **local** routing table.
  (Does **NOT** do iterative node lookup over the network.)
  """
  def closest_nodes(dht, id) do
    GenServer.call(dht, {:closest_nodes, id})
  end

  @doc """
  Update the routing table after receiving a message from `node`.
  If the node has the same ID as this node, the update is ignored.
  """
  def update(dht, node, stats \\ nil) do
    GenServer.call(dht, {:update, node, stats})
  end

  @doc """
  Append a new block to the ledger(s).
  """
  @deprecated "not implemented"
  def add_block(_dht, _block) do
    raise "FIXME: not implemented"
  end

  @doc """
  Retrieve the last (i.e. latest) block from the ledger for the account.
  """
  @deprecated "not implemented"
  def last_block(_dht, _account_id) do
    raise "FIXME: not implemented"
  end

  @impl GenServer
  def init(opts) do
    id = Keyword.fetch!(opts, :id)
    rpc = Keyword.fetch!(opts, :rpc)
    shard_size = Keyword.fetch!(opts, :shard_size)  # TODO: fallback default?
    lookup_opts = Keyword.get(opts, :lookup_opts, [])
      |> Keyword.put_new(:id, id)
      |> Keyword.put_new(:rpc, rpc)
      |> Keyword.put_new(:n_lookup, shard_size)
      |> Keyword.put_new(:max_pool, shard_size)
      |> Keyword.put_new(:alpha, opts[:alpha])
      |> Keyword.put_new(:probe_timeout, opts[:probe_timeout])
    state = %__MODULE__{
      id: id,
      rpc: rpc,
      rt_mod: Keyword.fetch!(opts, :rt_mod),
      rt: Keyword.fetch!(opts, :rt),
      lookup_opts: lookup_opts,
    }
    {:ok, state}
  end

  @impl GenServer
  def handle_call({:update, {id, _addr} = node, _stats}, _caller, state) do
    if id != state.id do
      state.rt_mod.update(state.rt, node)
    end
    {:reply, :ok, state}
  end

  @impl GenServer
  def handle_call({:join, opts}, caller, state) do
    spawn(fn ->
      bootstraps = opts[:bootstrap]
      result = _join(state, bootstraps)
      GenServer.reply(caller, result)
    end)
    {:noreply, state}
  end

  @impl GenServer
  def handle_call({:node_lookup, id}, _caller, state) do
    result = _node_lookup(state, id)
    {:reply, result, state}
  end

  @impl GenServer
  def handle_call({:closest_nodes, id}, _caller, state) do
    result = _closest_nodes(state, id)
    {:reply, result, state}
  end

  defp _closest_nodes(state, id) do
    state.rt_mod.closest(state.rt, id)
      |> Enum.map(&Contact.pair/1)
  end

  # TODO: Need to include self in the results.
  defp _node_lookup(state, id) do
    candidates = _closest_nodes(state, id)
    opts = state.lookup_opts
      |> Keyword.put(:id, id)
      |> Keyword.put(:target, id)
      |> Keyword.put(:candidates, candidates)
    result = NodeLookup.lookup(opts)
    result.result
  end

  # TODO: Bucket refresh. Needs some more settings/options.
  defp _join(state, bootstraps) do
    before_join = state.rt_mod.size(state.rt)
    if bootstraps do
      Enum.each(bootstraps, fn node ->
        state.rt_mod.update(state.rt, node)
      end)
    end
    # Look up self.
    peers = _node_lookup(state, state.id)
    after_join = state.rt_mod.size(state.rt)
    %{
      before: before_join,
      after: after_join,
      peers: peers,
      # TODO: more stats:
      # - how much bucket refresh did we do?
      # - how many did we find after doing bucket refresh?
      # - what's the total number of nodes we learned about?
      # - how many nodes did we introduces ourselves to? (contacts probed)
      # - which nodes did we find? What's our local neighborhood?
    }
  end

end
