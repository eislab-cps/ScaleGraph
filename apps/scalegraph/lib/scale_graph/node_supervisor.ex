defmodule ScaleGraph.NodeSupervisor do
  @moduledoc """
  The main module representing a ScaleGraph node. Creates a supervision tree
  with all process that make up a node.
  """
  use Supervisor

  def start_link(opts) do
    Supervisor.start_link(__MODULE__, opts, opts)
  end

  @impl Supervisor
  def init(opts) do
    default_net_name = :network_name
    default_rpc_name = :rpc_name
    default_rt_name = :rt_name
    default_dht_name = :dht_name
    default_node_name = :node_name

    # Get required options.
    {keys, opts} = Keyword.pop!(opts, :keys)
    {addr, opts} = Keyword.pop!(opts, :addr)
    {shard_size, opts} = Keyword.pop!(opts, :shard_size)

    # Get some options that may fall back on defaults.
    {id, opts} = Keyword.pop(opts, :id)  # TODO: get from keys if missing
    {bucket_size, opts} = Keyword.pop(opts, :bucket_size, shard_size)
    {net_mod, opts} = Keyword.pop(opts, :net_mod, Netsim.UDP)
    {rt_mod, opts} = Keyword.pop(opts, :rt_mod, ScaleGraph.DHT.FakeRT)

    # Get some options that may fall back on defaults.
    net_name = opts
      |> Keyword.get(:net_opts, [])
      |> Keyword.get(:name, default_net_name)
    rpc_name = opts
      |> Keyword.get(:rpc_opts, [])
      |> Keyword.get(:name, default_rpc_name)
    rt_name = opts
      |> Keyword.get(:rt_opts, [])
      |> Keyword.get(:name, default_rt_name)
    dht_name = opts
      |> Keyword.get(:dht_opts, [])
      |> Keyword.get(:name, default_dht_name)
    node_name = opts
      |> Keyword.get(:node_opts, [])
      |> Keyword.get(:name, default_node_name)


    # May or may not have some net opts, which may or may not have a name.
    {net_opts, opts} = Keyword.pop(opts, :net_opts, [])
    net_opts = net_opts
      |> Keyword.put_new(:name, net_name)
      |> Keyword.put_new(:connect, {addr, rpc_name})

    {rpc_opts, opts} = Keyword.pop(opts, :rpc_opts, [])
    rpc_opts = rpc_opts
      |> Keyword.put_new(:name, rpc_name)
      |> Keyword.put_new(:handler, node_name)
      |> Keyword.put_new(:net, {net_mod, net_name}) # needs mod, create or not!
      |> Keyword.put_new(:keys, keys)
      |> Keyword.put_new(:id, id)
      |> Keyword.put_new(:addr, addr)

    {rt_opts, opts} = Keyword.pop(opts, :rt_opts, [])
    rt_opts = rt_opts
      |> Keyword.put_new(:name, rt_name)
      |> Keyword.put_new(:bucket_size, bucket_size)
      |> Keyword.put_new(:id, id)

    {dht_opts, opts} = Keyword.pop(opts, :dht_opts, [])
    dht_opts = dht_opts
      |> Keyword.put_new(:name, dht_name)
      |> Keyword.put_new(:rpc, :rpc_name)
      |> Keyword.put_new(:rt, :rt_name)
      |> Keyword.put_new(:rt_mod, rt_mod)
      |> Keyword.put_new(:shard_size, shard_size)
      |> Keyword.put_new(:id, id)
    # TODO: What about alpha, max_pool, timeout etc?

    {node_opts, opts} = Keyword.pop(opts, :node_opts, [])
    node_opts = node_opts
      |> Keyword.put_new(:name, node_name)
      |> Keyword.put_new(:rpc, rpc_name)
      |> Keyword.put_new(:dht, dht_name)
      |> Keyword.put_new(:keys, keys)
      |> Keyword.put_new(:id, id)
      |> Keyword.put_new(:addr, addr)

    children = [
      {ScaleGraph.RPC, rpc_opts},
      {ScaleGraph.DHT.FakeRT, rt_opts},
      {ScaleGraph.DHT, dht_opts},
      {ScaleGraph.Node, node_opts},
    ]

    children =
      if net_mod == Netsim.UDP do
        [{net_mod, net_opts} | children]
      else
        children
      end
    Supervisor.init(children, opts)
  end

end
