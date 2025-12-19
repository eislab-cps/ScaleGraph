defmodule Demo do
  @moduledoc """
  Demo app currently limited to consensus (no DHT).

  Allows interactively setting up a network, making transactions, and
  viewing the state of nodes and accounts/clients. (Note that "clients" and
  "accounts" are used interchangably.)

  A network with a certain number of nodes and clients can be set up with
  `setup/1`. Individual nodes can be added at any time with `add_node/1` and
  `add_nodes/1`. Similarly, clients can be added with `add_client/1` and
  `add_clients/1`.
  A single transaction can be executed with `mk_tx/1`. See also `parallel_tx/1`
  and `dogpile/1`. The current state (of one or more nodes) can be shown with
  `show/1`. The shards (set of nodes) for each account can be shown with
  `shards/1`.

  When specifying nodes and accounts (for example which nodes to show using
  `show/1` or to specify the sender and/or receiver with `mk_tx/1`), their
  0-based index is used.
  """

  use GenServer
  alias Netsim.Fake
  alias ScaleGraph.Consensus
  alias ScaleGraph.Ledger
  alias Consensus.ShardConfig
  alias Consensus.ZftLock
  alias Ledger.Ledgers
  alias Ledger.Transaction
  import Abbrev

  # These are some "appropriate" RGB values. First, they were chosen so
  # that the differences between them are big enough to make them
  # relatively easy to distinguish. Second, they are defined in such an
  # order that the difference between the first few colors is bigger. So
  # for a small number of accounts, they are easy to distinguish. If there
  # is a larger number, then we start introducing increasingly more similar
  # colors.
  # TODO: If the number of accounts exceeds the number of colors, we
  # currently wrap around and start recycling colors. Considering that,
  # perhaps an equally good solution is to just include more and more
  # nuances, even if they are difficult to distinguish (after all, reusing
  # exactly the same color doesn't exactly make it easy to distinguish).
  @rgb [
    {1,1,5}, {1,5,1}, {5,1,1},
    {1,5,5}, {5,5,1}, {5,1,5},
    {1,3,5}, {3,5,1}, {5,1,3}, {3,1,5}, {5,3,1},
    {1,1,3}, {1,3,1}, {3,1,1},
    {1,3,3}, {3,3,1}, {3,1,3},
    {1,3,3}, {3,3,1}, {3,1,3},
    {1,1,1}, {3,3,3}, {5,5,5},
    {3,5,5}, {5,5,3}, {5,3,5},
  ]
  @colors Enum.map(@rgb, fn {r,g,b} -> "#{IO.ANSI.color(r,g,b)}" end)
  @reset IO.ANSI.reset()
  @bold IO.ANSI.bright()
  @normal IO.ANSI.normal()
  @blue IO.ANSI.color(0,2,2)
  @orange IO.ANSI.color(3,1,0)
  @gray IO.ANSI.color(2,2,2)

  @default_shard_size 3
  @default_id_bits 32

  @doc false
  def start(opts) do
    GenServer.start(__MODULE__, opts, name: __MODULE__)
  end

  @impl GenServer
  def init(opts) do
    shard_size = Keyword.get(opts, :shard_size, @default_shard_size)
    id_bits = Keyword.get(opts, :id_bits, @default_id_bits)
    {:ok, net} = Fake.start_link([])
    {:ok, shards} = FakeDht.start_link(shard_size: shard_size)
    state = %{
      shard_size: shard_size,
      id_bits: id_bits,
      net: net,
      shards: shards,
      # Map index to node (id, addr, ledgers, consensus).
      nodes_by_index: %{},
      # Map node ID to node (id, addr, ledgers, consensus).
      nodes_by_id: %{},
      clients_by_index: %{},
      clients_by_id: %{},
      # What nodes are in the shard for each account?
      account_to_nodes: %{},
      # What accounts is each node near to? (What shards is it in?)
      node_to_accounts: %{},
    }
    {:ok, state}
  end

  @doc """
  Reset the network. This will remove all nodes and clients.

  Optional arguments:
  - `:shard_size` sets the shard size.
  - `:id_bits` sets the size of IDs (number of bits).
  """
  def reset(opts \\ []) do
    if not is_nil(GenServer.whereis(__MODULE__)) do
      GenServer.stop(__MODULE__, :normal)
    end
    {:ok, _} = start(opts)
    show()
  end

  # TODO: Should probably warn if there are already nodes/clients.
  @doc """
  Add the specified number of nodes and clients/accounts. The default number of
  nodes is twice the shard size. The default number of clients is the shard
  size. By default, the resulting state is shown (calling `show/1`).
  To suppress, pass `:noshow`, e.g. `setup([:noshow, nodes: 4, accounts: 3])`.

  Optional arguments:
  - `:nodes` the number of nodes to add.
  - `:clients` or `:accounts` the number of clients/accounts to add.
  - `:shard_size` sets the shard size **if not already running**.
  - `:id_bits` sets the size of IDs (number of bits) **if not already running**.

  Simply calls `add_nodes/1` and `add_clients/1`. Note that this means
  nodes/clients will be added whether or not some already exist!
  """
  def setup(opts \\ []) do
    _nop_if_running = start(opts)
    shard_size = Keyword.get(opts, :shard_size, @default_shard_size)
    n_nodes = Keyword.get(opts, :nodes) || 2*shard_size
    n_clients = Keyword.get(opts, :clients) || Keyword.get(opts, :accounts) || shard_size
    add_nodes(n_nodes)
    add_clients(n_clients)
    noshow? = :noshow in opts || Keyword.get(opts, :noshow)
    unless noshow? do
      show()
    end
    :ok
  end

  # TODO: Allow a range or list of nodes to be specified?
  @doc """
  Show the current state of nodes (and the accounts they store).
  If given a node number (0-indexed), shows the state of that node.
  Otherwise, shows all nodes.

  Options:
  - `:noold` - hide accounts for which the node is no longer in the shard.
  - `:notx` - only show the current account balance, not the list of transactions.
  - `:brief` - equivalent to `:noold` and `:notx`.

  Note that the transactions that are shown for different accounts show the
  amount as `+amount` and `-amount`. This is just to more clearly show where
  money is sent or received. (Amounts are never actually negative.)
  """
  def show(opts \\ []) do
    GenServer.call(__MODULE__, {:show, opts})
  end

  # TODO: Might also want to show for each node which other nodes it shares a
  # shard with and some stats about how many shards each node is in (min, max, avg).
  @doc """
  Show the shards (a list of nodes for each account).
  """
  def shards(opts \\ []) do
    GenServer.call(__MODULE__, {:shards, opts})
  end

  @doc """
  Show system info:
  parameters, current number of nodes and clients, processes, and memory.
  """
  def sysinfo() do
    GenServer.call(__MODULE__, :sysinfo)
  end

  @doc "Add `n` new random nodes. Simply calls `add_node/1` `n` times."
  def add_nodes(n) do
    Enum.map(1..n, fn _ -> add_node() end)
  end

  @doc """
  Add a new node, optionally setting ID or address.
  If omitted, a random ID and/or address is generated.
  Every new node (except the first) uses the first node as bootstrap.

  Options:
  - `id: id` - sets the node ID to the hash of the given `id`.
  - `addr: {{a, b, c, d}, p}` - sets the address to a.b.c.d:p.
  - `:nohash` - use the `id` as-is instead of hashing it.

  Note that specifying an ID with `id: id` is a way to make IDs deterministic.
  But to make them meaningful (in terms of computing distances and forming
  shards) they need to "look random", which is why they are hashed by default.
  To use the precise/raw ID, specify the `:nohash` flag.
  """
  def add_node(opts \\ [])

  # Interpret `add_node(5)` as `add_nodes(5)` rather than crashing when
  # 5 turns out not to be a valid keyword list.
  def add_node(n) when is_integer(n) do
    add_nodes(n)
  end

  def add_node(opts) do
    GenServer.call(__MODULE__, {:add_node, opts})
  end

  @doc "Add `n` new random clients. Simply calls `add_client/1` `n` times."
  def add_clients(n) do
    Enum.map(1..n, fn _ -> add_client() end)
  end

  @doc """
  Add a new client/account, optionally setting ID or address.
  If omitted, a random ID and/or address is generated.

  Options:
  - `id: id` - sets the account ID to the hash of the given `id`.
  - `addr: {{a, b, c, d}, p}` - sets the address to a.b.c.d:p.
  - `:nohash` - use the `id` as-is instead of hashing it.

  See also `add_node/1`.
  """
  def add_client(opts \\ [])

  # Interpret `add_client(5)` as `add_clients(5)` rather than crashing when
  # 5 turns out not to be a valid keyword list.
  def add_client(n) when is_integer(n) do
    add_clients(n)
  end

  def add_client(opts) do
    GenServer.call(__MODULE__, {:add_client, opts})
  end

  @doc """
  Make a client submit a transaction.
  For any optional arguments that are not specified, some sensible default
  (or randomly generated value) will be used.

  Optional arguments:
  - `from: index` the sender account/client.
  - `to: index` the receiver account/client.
  - `amount: amount` the amount to transfer.
  - `nonce: number` the transaction nonce.
  """
  def mk_tx(opts \\ []) do
    GenServer.call(__MODULE__, {:mk_tx, opts})
  end

  @doc """
  Make `n` parallel transactions. By default, `n` is the number of pairs of
  accounts, i.e. the largest number of parallel transactions possible.
  """
  def parallel_tx(n \\ nil)

  def parallel_tx([n: n]) do
    # Most other functions take keyword lists. For consistency, support it here.
    parallel_tx(n)
  end

  def parallel_tx(n) do
    GenServer.call(__MODULE__, {:parallel_tx, n})
  end

  @doc """
  Make `:n` simultaneous transactions to account `:to`.
  If no `:to` is specified, a random receiver is chosen.
  If `:n` is not specified, **all** other accounts will make transactions to `to`.

  Optional arguments:
  - `to: index` the receiver account
  - `n: count` the number of transactions.
  """
  def dogpile(opts \\ []) do
    GenServer.call(__MODULE__, {:dogpile, opts})
  end


  @impl GenServer
  def handle_call({:add_node, opts}, _caller, state) do
    index = map_size(state.nodes_by_index)
    node_id = _get_id(state, opts)
    node_addr = _get_addr(state, opts)
    FakeDht.add_node(state.shards, {node_id, node_addr})
    {:ok, ledgers} = Ledgers.start_link([])
    consensus_opts = [
      node_id: node_id,
      node_addr: node_addr,
      ledgers: ledgers,
      config_lookup: state.shards,
      netmod: Fake,
      net: state.net,
      protocol: ZftLock,
    ]
    {:ok, consensus} = Consensus.start_link(consensus_opts)
    node = %{
      index: index,
      node_id: node_id,
      node_addr: node_addr,
      ledgers: ledgers,
      consensus: consensus,
    }
    state = put_in(state.nodes_by_index[index], node)
    state = put_in(state.nodes_by_id[node_id], node)
    state = _update_shards(state)
    state = _fake_replication(state, node)
    result = {index, {node_id, node_addr}}
    {:reply, result, state}
  end

  @impl GenServer
  def handle_call({:add_client, opts}, _caller, state) do
    account_id = _get_id(state, opts)
    client_addr = _get_addr(state, opts)
    index = map_size(state.clients_by_index)
    client = %{
      index: index,
      account_id: account_id,
      nonce: 0,
    }
    state = put_in(state.clients_by_index[index], client)
    state = put_in(state.clients_by_id[account_id], client)
    state = _update_shards(state)
    # Add the account to the nodes in its shard.
    shard = state.account_to_nodes[account_id]
    Enum.each(shard, fn node_id ->
      ledgers = state.nodes_by_id[node_id].ledgers
      Ledgers.create(ledgers, account_id)
    end)
    result = {index, {account_id, client_addr}}
    {:reply, result, state}
  end

  @impl GenServer
  def handle_call({:mk_tx, opts}, _caller, state) do
    tx = _build_tx(state, opts)
    state = _send_tx(state, tx)
    {:reply, tx, state}
  end

  @impl GenServer
  def handle_call({:parallel_tx, n}, _caller, state) do
    n = n || div(map_size(state.clients_by_index), 2)
    state = Enum.map(0..(n-1), fn i -> _build_tx(state, from: i*2, to: i*2+1) end)
      |> Enum.reduce(state, &_send_tx(&2, &1))
    {:reply, :ok, state}
  end


  # FIXME: what if :n >= number of clients?
  @impl GenServer
  def handle_call({:dogpile, opts}, _caller, state) do
    n = Keyword.get(opts, :n) || map_size(state.clients_by_index) - 1
    to = Keyword.get(opts, :to) || _random_client_index(state)
    state =
      Enum.filter(0..n, &(&1 != to))
        |> Enum.map(fn i -> _build_tx(state, from: i, to: to) end)
        |> Enum.reduce(state, &_send_tx(&2, &1))
    {:reply, :ok, state}
  end

  @impl GenServer
  def handle_call({:show, opts}, _caller, state) do
    IO.puts("#{map_size(state.nodes_by_index)} nodes, #{map_size(state.clients_by_index)} clients/accounts\n")
    index = Keyword.get(opts, :node)
    indices =
      if is_nil(index) do
        Enum.sort(Map.keys(state.nodes_by_index))
      else
        [index]
      end
    Enum.each(indices, &_show_node(state, &1, opts))
    {:reply, :ok, state}
  end

  @impl GenServer
  def handle_call({:shards, _opts}, _caller, state) do
    Enum.sort(Map.keys(state.clients_by_index))
    |> Enum.map(fn index ->
      account_id = state.clients_by_index[index].account_id
      color = _get_account_color(state, account_id)
      IO.puts("#{color}#{@bold}#{index}: #{abbrev(account_id)}#{@reset}")
      shard = state.account_to_nodes[account_id]
        |> Enum.map(fn node_id ->
          {state.nodes_by_id[node_id].index, node_id}
        end)
        |> Enum.sort()
      Enum.each(shard, fn {node_index, node_id} ->
        color = _get_node_color(state, node_index)
        IO.puts("  #{color}#{@bold}Node #{node_index}#{@normal} #{abbrev(node_id)}#{@reset}")
      end)
    end)
    shardless_nodes =
      Enum.filter(state.node_to_accounts, fn {_node_id, accounts} ->
        length(accounts) == 0
      end)
    if length(shardless_nodes) > 0 do
      IO.puts("Shardless nodes:")
      Enum.map(shardless_nodes, fn {node_id, _} ->
        {state.nodes_by_id[node_id].index, node_id}
      end)
      |> Enum.sort()
      |> Enum.each(fn {node_index, node_id} ->
          IO.puts("  #{@gray}#{@bold}Node #{node_index}#{@normal} #{abbrev(node_id)}#{@reset}")
        end)
    end
    {:reply, :ok, state}
  end

  @impl GenServer
  def handle_call(:sysinfo, _caller, state) do
    IO.puts("#{@blue}shard size: #{@orange}#{state.shard_size}")
    IO.puts("#{@blue}ID bits: #{@orange}#{state.id_bits}")
    IO.puts("#{@blue}nodes: #{@orange}#{map_size(state.nodes_by_index)}")
    IO.puts("#{@blue}clients: #{@orange}#{map_size(state.clients_by_index)}")
    processes = :erlang.system_info(:process_count)
    max_processes = :erlang.system_info(:process_limit)
    IO.puts("#{@blue}processes: #{@orange}#{processes} (#{:erlang.float_to_binary(processes / max_processes * 100, [decimals: 2])} %)")
    IO.puts("#{@blue}memory: #{@orange}#{div(:erlang.memory(:total), 1_000_000)} MB")
    {:reply, :ok, state}
  end

  @doc false
  defp _show_node(state, index, opts) do
    brief? = :brief in opts || Keyword.get(opts, :brief)
    hide_old? = :noold in opts || Keyword.get(opts, :noold) || brief?
    notx? = :notx in opts || Keyword.get(opts, :notx) || brief?
    node = state.nodes_by_index[index]
    IO.puts("#{@bold}Node #{index}#{@normal} #{abbrev(node.node_id)}")
    %{ledgers: ledgers} = :sys.get_state(node.ledgers)
    ledgers = Map.keys(state.clients_by_index)
      |> Enum.sort()
      |> Enum.map(fn i -> state.clients_by_index[i].account_id end)
      |> Enum.filter(&Map.has_key?(ledgers, &1))
      |> Enum.map(&Map.get(ledgers, &1))

    Enum.each(ledgers, fn ledger ->
      account = Ledger.account(ledger)
      id = account.id
      index = _get_account_index(state, id)
      color = _get_account_color(state, id)
      fbalance = String.pad_leading(Integer.to_string(account.balance), 4, " ")
      # Is the node still in the shard? I.e., is the ledger still current?
      # If not, show the account info in gray.
      old? = node.node_id not in state.account_to_nodes[id]
      header =
        if old? do
          "#{@gray}#{index}: #{abbrev(id)}: #{fbalance}#{@reset}"
        else
          "#{@bold}#{color}#{index}#{@normal}: #{abbrev(id)}#{@reset}: #{@bold}#{fbalance}#{@normal}"
        end
      cond do
        old? and hide_old? ->
          nil
        notx? ->
          IO.puts("  #{header}")
        :else ->
          txs = _format_ledger(state, id, ledger, old?)
          IO.puts("  #{header}  [#{txs}]")
      end
    end)
  end

  defp _get_account_index(state, account_id) do
    state.clients_by_id[account_id].index
  end

  # XXX: takes an account **ID**
  defp _get_account_color(state, account_id) do
    index = _get_account_index(state, account_id)
    n = length(@colors)
    Enum.at(@colors, rem(index, n))
  end

  # XXX: takes a node **index**
  defp _get_node_color(_state, node_index) do
    n = length(@colors)
    Enum.at(@colors, rem(node_index, n))
  end

  defp _format_ledger(state, account_id, ledger, old?) do
    blocks = :sys.get_state(ledger).blocks
    Enum.reverse(blocks)
    |> Enum.map(fn block ->
      tx = block.body.tx
      _format_tx(state, account_id, tx, old?)
    end)
    |> Enum.join(" | ")
  end

  defp _format_tx(state, account_id, tx, old?) do
    snd = tx.snd
    rcv = tx.rcv
    bold = (old? && "") || @bold
    snd_color = (old? && @gray) || _get_account_color(state, snd)
    rcv_color = (old? && @gray) || _get_account_color(state, rcv)
    amount = cond do
      snd == account_id -> "-#{tx.amt}"
      true -> "+#{tx.amt}"
    end
    snd = _get_account_index(state, snd)
    rcv = _get_account_index(state, rcv)
    snd = "#{snd_color}#{snd}#{@reset}"
    rcv = "#{rcv_color}#{rcv}#{@reset}"
    amount = "#{bold}#{amount}#{@normal}"
    #"#{snd} → #{rcv}, #{amount}"
    "#{tx.nce}: #{snd} → #{rcv}, #{amount}"
  end

  defp _update_shards(state) do
    account_ids = Map.keys(state.clients_by_id)
    account_to_nodes = account_ids
      #|> Enum.map(fn account -> {account, ShardConfig.get(state.shards, account)} end)
      |> Enum.map(&({&1, ShardConfig.get(state.shards, &1)}))
      |> Enum.map(fn {account, shard} ->
          ids = Enum.map(shard, fn {id, _} -> id end)
          {account, ids}
        end)
      |> Map.new()
    node_ids = Map.keys(state.nodes_by_id)
    node_to_accounts = node_ids
      |> Enum.map(fn id -> {id, []} end)
      |> Map.new()
    node_to_accounts = Enum.reduce(account_to_nodes, node_to_accounts, fn {account, node_ids}, acc ->
      Enum.reduce(node_ids, acc, fn id, acc2 ->
        old = acc2[id]
        new = [account | old]
        Map.put(acc2, id, new)
      end)
    end)
    %{state |
      account_to_nodes: account_to_nodes,
      node_to_accounts: node_to_accounts,
    }
  end

  # Fetch an up-to-date ledger from another node in the same shard.
  # Use it to get the new node's ledger up-to-date.
  defp _fake_replication(state, %{node_id: node_id, ledgers: ledgers}=new_node) do
    shard_peers = Enum.map(state.account_to_nodes, fn {account, shard} ->
      peers = Enum.filter(shard, &(&1 != node_id))
      {account, peers}
    end)
    # Create an empty ledger in the new node for each account
    Enum.each(shard_peers, fn {account, _peers} ->
      Ledgers.create(ledgers, account)
    end)
    # Now filter out shards where this node is the only node.
    shard_peers = shard_peers
      |> Enum.filter(fn {_account, peers} ->
        length(peers) > 0
      end)
    # Pick one peer in each shard to copy the blocks over.
    Enum.each(shard_peers, fn {account, peers} ->
      peer = hd(peers)
      peer_ledger = :sys.get_state(state.nodes_by_id[peer].ledgers).ledgers[account]
      peer_blocks = :sys.get_state(peer_ledger).blocks
      node_ledger = :sys.get_state(new_node.ledgers).ledgers[account]
      Enum.each(peer_blocks, &Ledger.commit(node_ledger, &1))
    end)
    state
  end

  # FIXME: does not take into account which accounts can affort what amount!
  defp _build_tx(state, opts) do
    {snd_i, rcv_i} = _get_tx_pair(state, opts)
    amount = _get_tx_amount(state, opts)
    snd = state.clients_by_index[snd_i].account_id
    rcv = state.clients_by_index[rcv_i].account_id
    nonce = _get_tx_nonce(state, snd, opts)
    Transaction.new(snd, rcv, amount, nonce)
  end

  defp _send_tx(state, tx) do
    leader = _get_leader(state, tx.snd)
    Consensus.process_tx(leader.consensus, {tx, nil})
    # XXX: Assuming the TX is successful! What if it isn't? Then the nonce is off!
    put_in(state.clients_by_id[tx.snd].nonce, state.clients_by_id[tx.snd].nonce + 1)
  end

  defp _get_leader(state, account_id) do
    shard = ShardConfig.get(state.shards, account_id)
    {id, _addr} = hd(shard)
    state.nodes_by_id[id]
  end


  # XXX: Dumb way to avoid sending to self. TODO: Be more clever.
  defp _get_tx_pair(state, opts) do
    snd = _get_tx_snd(state, opts)
    rcv = _get_tx_rcv(state, opts)
    if snd == rcv do
      _get_tx_pair(state, opts) # try again
    else
      {snd, rcv}
    end
  end

  defp _get_tx_snd(state, opts), do:
    Keyword.get(opts, :from) || _random_client_index(state)

  defp _get_tx_rcv(state, opts), do:
    Keyword.get(opts, :to) || _random_client_index(state)

  defp _get_tx_amount(_state, opts), do:
    Keyword.get(opts, :amount) || _random_amount()

  defp _get_tx_nonce(state, snd_id, opts) do
    Keyword.get(opts, :nonce) || state.clients_by_id[snd_id].nonce
  end

  defp _get_id(state, opts) do
    id = Keyword.get(opts, :id) || _random_id(state.id_bits)
    nohash? = :nohash in opts || Keyword.get(opts, :nohash)
    if nohash? do
      <<id::size(state.id_bits)>>
    else
      <<id::size(state.id_bits)-bitstring, _::bitstring>> = Crypto.sha256(:erlang.term_to_binary(id))
      id
    end
  end

  defp _get_addr(_state, opts) do
    Keyword.get(opts, :addr) || _random_address()
  end

  defp _random_amount() do
    Util.rand(10, 50)
  end

  defp _random_client_index(state) do
    max_index = map_size(state.clients_by_index) - 1
    Util.rand(0, max_index)
  end

  defp _random_id(b) do
    Util.rand_bits(b)
  end

  defp _random_address() do
    o1 = Util.rand(100, 255)
    o2 = Util.rand(10, 99)
    o3 = Util.rand(10, 99)
    o4 = Util.rand(10, 99)
    port = Util.rand(1000, 9999)
    {{o1, o2, o3, o4}, port}
  end

end
