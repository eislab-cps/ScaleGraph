defmodule ScaleGraph.Consensus.StaticConfig do
  @moduledoc """
  A static shard config.

  This is useful for testing, but it also matches how statically configured
  consensus protocols would work, where the set of participating servers is
  known ahead of time.
  """
  use GenServer

  def start_link(opts) do
    {shards, opts} = Keyword.pop(opts, :shards, %{})
    GenServer.start_link(__MODULE__, [shards: shards], opts)
  end

  def add(config, account_id, nodes) do
    GenServer.call(config, {:add_config, account_id, nodes})
  end

  def get(config, account_id) do
    GenServer.call(config, {:get_config, account_id})
  end

  @impl GenServer
  def init(opts) do
    state = Keyword.fetch!(opts, :shards)
    {:ok, state}
  end

  @impl GenServer
  def handle_call({:add_config, account_id, nodes}, _caller, state) do
    state = Map.put(state, account_id, nodes)
    {:reply, :ok, state}
  end

  @impl GenServer
  def handle_call({:get_config, account_id}, _caller, state) do
    shard = state[account_id]
    {:reply, shard, state}
  end

end
