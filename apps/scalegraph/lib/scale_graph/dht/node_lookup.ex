# XXX: A GenServer may not be the best choice for this.
# TODO:
# - Consider allowing a soft timeout to be set and launch a new probe when this
#   timeout is triggered.
# - Allow sort/probe order to be specified.
#
# NOTE: NodeLookup knows nothing about the RT. It just takes an initial pool of
# candidates and finds new ones by making RPCs.
defmodule ScaleGraph.DHT.NodeLookup do
  @moduledoc """
  Performs Kademlia-style node lookup. See `lookup/1`.

  Initially, `alpha` probes are sent. Each reply (or timeout) launches another
  probe. Lookup terminates when the pool of candidates has been exhausted.
  (Nobody was able to suggest more/better candidates.)

  Currently, the order in which candidates are probed is not specified.
  """
  use GenServer
  require Logger
  alias ScaleGraph.RPC

  @doc """
  Run lookup.

  Required options:
  - `:id` - the ID of the local node.
  - `:rpc` - the RPC server to use for sending requests.
  - `:n_lookup` - the number of nodes to find.
  - `:target` - the ID to look up.
  - `:candidates` - the initial pool of candidates (list of `{id, addr}` pairs).

  Optional:
  - `:alpha` - the number of parallel requests (2 by default).
  - `:max_pool` - maximum candidate pool size (`:n_lookup` by default).
  - `:probe_timeout` - how long (in ms) to wait for a response (500 by default).
  """
  def lookup(opts) do
    {:ok, pid} = GenServer.start_link(__MODULE__, opts, opts)
    GenServer.call(pid, :lookup)
  end

  @impl GenServer
  def init(opts) do
    state = %{
      id: Keyword.fetch!(opts, :id),
      rpc: Keyword.fetch!(opts, :rpc),
      target: Keyword.fetch!(opts, :target),
      candidates: Keyword.fetch!(opts, :candidates),
      n_lookup: Keyword.fetch!(opts, :n_lookup),
      alpha: opts[:alpha] || 2,
      max_pool: opts[:max_pool] || opts[:n_lookup],
      timeout: Keyword.get(opts, :probe_timeout, 500),
      probed: MapSet.new(),
      alive: MapSet.new(),
      inflight: 0,
    }
    {:ok, state}
  end

  @impl GenServer
  def handle_call(:lookup, caller, state) do
    # TODO: Move this check to DHT? (Don't call lookup with empty pool!)
    if length(state.candidates) == 0 do
      Logger.warning("lookup with EMPTY candidate pool")
      result = %{
        probed: [],
        alive: [],
        result: [],
      }
      GenServer.reply(caller, result)
      {:stop, :normal, state}
    else
      state = Map.put(state, :caller, caller)
      state = Enum.reduce(1..state.alpha, state, fn _, state ->
        send_probe(state)
      end)
      {:noreply, state}
    end
  end

  @impl GenServer
  def handle_info({:timeout, _}, state) do
    state = Map.put(state, :inflight, state[:inflight]-1)
    # TODO: This is duplicated below. Refactor!
    if done?(state) do
      result = state.alive
        |> Enum.sort_by(fn {id, _addr} -> Util.distance(id, state.target) end)
        |> Enum.take(state.n_lookup)
      GenServer.reply(state.caller, result)
      {:stop, :normal, state}
    else
      state = send_probe(state)
      {:noreply, state}
    end
  end

  @impl GenServer
  def handle_info({:rpc_response, _} = resp, state) do
    new_candidates = RPC.data(resp)
    old_set = MapSet.new(state.candidates)
    new = Enum.filter(new_candidates, fn {id, _addr} = node ->
      !MapSet.member?(state.probed, node) &&
      !MapSet.member?(old_set, node) &&
      id != state.id
    end)
    # Maintains candidates in sorted order by (ascending) distance.
    # (This should probably be configurable.)
    new_candidates =
      state.candidates ++ new
      |> Enum.sort_by(fn {id, _addr} -> Util.distance(id, state.target) end)
      |> Enum.take(state.max_pool)
    alive = MapSet.put(state.alive, RPC.src(resp))
    state = %{state |
      candidates: new_candidates,
      alive: alive,
      inflight: state.inflight - 1,
    }
    if done?(state) do
      result = state.alive
        |> Enum.sort_by(fn {id, _addr} -> Util.distance(id, state.target) end)
        |> Enum.take(state.n_lookup)
      result = %{
        probed: state.probed,
        alive: state.alive,
        result: result,
      }
      GenServer.reply(state.caller, result)
      {:stop, :normal, state}
    else
      state = send_probe(state)
      {:noreply, state}
    end
  end

  defp done?(state) do
    ([] == state.candidates) && (state.inflight == 0)
  end

  # No more candidates to probe!
  defp send_probe(%{candidates: []} = state) do
    state
  end

  defp send_probe(state) do
    [next | rest] = state.candidates
    RPC.find_nodes(state.rpc, next, state.target, timeout: state.timeout)
    probed = MapSet.put(state.probed, next)
    inflight = state.inflight + 1
    %{state | candidates: rest, probed: probed, inflight: inflight}
  end

end
