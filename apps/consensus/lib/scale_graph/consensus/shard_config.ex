defmodule ScaleGraph.Consensus.ShardConfig do
  @moduledoc """
  Look up shard configs (membership).
  """

  def add(config, account_id, nodes) do
    GenServer.call(config, {:add_config, account_id, nodes})
  end

  def get(config, account_id) do
    GenServer.call(config, {:get_config, account_id})
  end

end
