# Works a bit like RPC, but for consensus messages.
defmodule ScaleGraph.Consensus do
  @moduledoc """
  The consensus module is responsible for sharded consensus.

  Accepts requests for initiating consensus decisions, i.e. processing
  transactions, from the application. It also has a connection to the
  network and can receive consensus messages from the network and route
  them to the appropriate consensus instance.

  Currently, this module is very simple, and its responsibilities are
  essentially limited to routing messages. Future versions will take on more
  responsibility when we have "static" (i.e. long-running) shard-specific
  protocol instances for locking and "dynamic" (i.e. ad hoc spun-up)
  validator group instances for TX consensus.
  """
  use GenServer
  alias ScaleGraph.Consensus.Instance
  alias ScaleGraph.Ledger
  alias Ledger.Ledgers
  import Abbrev
  require Logger

  defstruct [
    me: nil,
    instances: %{}, # TODO: Replace with dynamic supervisor
    ledgers: nil,
    config_lookup: nil,
    net: nil,
    netmod: nil,
    # TODO: need to clean this if there is no response
    expecting: %{},
    protocol: nil,
    shard_size: nil,
  ]

  def start_link(opts) do
    keys = [:node_id, :node_addr, :ledgers, :config_lookup, :netmod, :net, :protocol, :shard_size]
    {consensus_opts, gen_server_opts} = Keyword.split(opts, keys)
    GenServer.start_link(__MODULE__, consensus_opts, opts)
  end

  # This is called by Node after receiving a SUBMIT-TX.
  @doc """
  Run consensus on transaction `tx` (with client signature `sig`).
  """
  def process_tx(consensus, {tx, sig}) do
    GenServer.cast(consensus, {:process_tx, tx, sig})
  end

  @doc """
  Send a request for the last block header to a node in the receiver shard.
  """
  def request_last_block_info(consensus, account1=src, {{id2, addr2}, account2}=dst) do
    GenServer.call(consensus, {:last_block, src, dst})
  end

  @doc """
  Send a (synchronous, blocking) request to lock an account/shard.
  The function will return once the response has been received.
  """
  def request_receiver_lock_sync(consensus, account1=src, {{id2, addr2}, account2}=dst, tx) do
    GenServer.call(consensus, {:receiver_lock_sync, src, dst, tx})
  end

  @doc "Send a request (asynchronously) to lock an account/shard."
  def request_receiver_lock_async(consensus, account1=src, {{id2, addr2}, account2}=dst, tx) do
    GenServer.cast(consensus, {:receiver_lock_async, src, dst, tx})
  end

  @doc "Reply to a lock request."
  def answer_lock_request(consensus, account1=src, {{id2, addr2}, account2}=dst, result) do
    GenServer.cast(consensus, {:receiver_lock_response, src, dst, result})
  end

  @doc """
  **Deprecated.**
  """
  def request_receiver_lock(consensus, account1=src, {{id2, addr2}, account2}=dst, tx) do
    GenServer.call(consensus, {:receiver_lock_sync, src, dst, tx})  # sync
  end

  @doc "Send a propose message."
  def propose(consensus, account1=src, {{id2, addr2}, account2}=dst, prop) do
    #msg = ScaleGraph.Consensus.Msg.proposal(src, dst, prop)
    #payload = encode(msg)
    #state.netmod.send(state.net, addr2, payload)
    GenServer.cast(consensus, {:propose, src, dst, prop})
  end

  @doc "Send a vote message."
  def vote(consensus, account1=src, {{id2, addr2}, account2}=dst, prop) do
    GenServer.cast(consensus, {:vote, src, dst, prop})
  end

  @doc "Send a commit message."
  def commit(consensus, account1=src, {{id2, addr2}, account2}=dst, prop) do
    GenServer.cast(consensus, {:commit, src, dst, prop})
  end

  @doc "Encode a consensus message to binary for transmission over the network."
  def encode(msg), do: :erlang.term_to_binary(msg)

  @doc "Decode a binary consensus message received from the network."
  def decode(msg) when is_binary(msg), do: :erlang.binary_to_term(msg)

  @impl GenServer
  def init(opts) do
    node_id = Keyword.fetch!(opts, :node_id)
    node_addr = Keyword.fetch!(opts, :node_addr)
    netmod = Keyword.fetch!(opts, :netmod)
    net = Keyword.fetch!(opts, :net)
    netmod.connect(net, node_addr)
    state = %__MODULE__{
      me: {node_id, node_addr},
      ledgers: Keyword.fetch!(opts, :ledgers),
      config_lookup: Keyword.fetch!(opts, :config_lookup),
      netmod: netmod,
      net: net,
      protocol: Keyword.fetch!(opts, :protocol),
      # FIXME: fetch! The shard size should be there!
      shard_size: Keyword.get(opts, :shard_size),
    }
    {:ok, state}
  end

  @impl GenServer
  def handle_info({:network, payload}, state) do
    msg = decode(payload)
    {:consensus, {tag, {{id1, addr1}, account1}=src, {{id2, _addr2}, account2}=dst, data}} = msg
    case tag do
      :req_last_block ->
        {:ok, ledger} = Ledgers.ledger(state.ledgers, account2)
        result = Ledger.last_block_info(ledger)
        msg = ScaleGraph.Consensus.Msg.ret_last_block(dst, src, result)
        payload = encode(msg)
        state.netmod.send(state.net, addr1, payload)
        {:noreply, state}
      :ret_last_block ->
        {caller, expecting} = Map.pop!(state.expecting, {{id2, account2}, {id1, account1}})
        state = Map.put(state, :expecting, expecting)
        GenServer.reply(caller, data)
        {:noreply, state}
      :ret_lock ->
        if Map.has_key?(state.expecting, {{id2, account2}, {id1, account1}}) do
          # SYNC
          {caller, expecting} = Map.pop!(state.expecting, {{id2, account2}, {id1, account1}})
          state = Map.put(state, :expecting, expecting)
          GenServer.reply(caller, data)
          {:noreply, state}
        else
          # ASYNC
          {instance, state} = _get_instance(state, account2)
          GenServer.cast(instance, {tag, src, data})
          {:noreply, state}
        end
      _else ->
        {instance, state} = _get_instance(state, account2)
        GenServer.cast(instance, {tag, src, data})
        {:noreply, state}
    end
  end

  @impl GenServer
  def handle_call({:last_block, account1, {{id2, addr2}, account2}=dst}, caller, state) do
    src = {state.me, account1}
    {id, _addr} = state.me
    msg = ScaleGraph.Consensus.Msg.req_last_block(src, dst)
    payload = encode(msg)
    state.netmod.send(state.net, addr2, payload)
    # TODO: using {{id1, account1}, {id2, account2}} as the key is overkill!
    # Each consensus instance only handles one TX at a time. So just the local
    # (i.e. requesting) instance (account) should be sufficient.
    state = put_in(state.expecting[{{id, account1}, {id2, account2}}], caller)
    {:noreply, state}
  end

  # ASYNC
  @impl GenServer
  def handle_cast({:receiver_lock_async, account1, {{id2, addr2}, account2}=dst, tx}, state) do
    src = {state.me, account1}
    {id, _addr} = state.me
    msg = ScaleGraph.Consensus.Msg.req_lock(src, dst, tx)
    payload = encode(msg)
    state.netmod.send(state.net, addr2, payload)
    {:noreply, state}
  end

  # SYNC
  @impl GenServer
  def handle_call({:receiver_lock_sync, account1, {{id2, addr2}, account2}=dst, tx}, caller, state) do
    src = {state.me, account1}
    {id, _addr} = state.me
    msg = ScaleGraph.Consensus.Msg.req_lock(src, dst, tx)
    payload = encode(msg)
    state.netmod.send(state.net, addr2, payload)
    # TODO: using {{id1, account1}, {id2, account2}} as the key is overkill!
    # Each consensus instance only handles one TX at a time. So just the local
    # (i.e. requesting) instance (account) should be sufficient.
    state = put_in(state.expecting[{{id, account1}, {id2, account2}}], caller)
    {:noreply, state}
  end

  @impl GenServer
  def handle_cast({:receiver_lock_response, account1, {{id2, addr2}, account2}=dst, result}, state) do
    src = {state.me, account1}
    {id, _addr} = state.me
    msg = ScaleGraph.Consensus.Msg.ret_lock(src, dst, result)
    payload = encode(msg)
    state.netmod.send(state.net, addr2, payload)
    {:noreply, state}
  end

  # TODO: These three (propose, vote, commit) should probably just take a binary
  # payload and send it over the network.
  # Building the message is stateless. Only need state to access the network.

  @impl GenServer
  def handle_cast({:propose, account1, {{id2, addr2}, account2}=dst, prop}, state) do
    src = {state.me, account1}
    msg = ScaleGraph.Consensus.Msg.proposal(src, dst, prop)
    payload = encode(msg)
    state.netmod.send(state.net, addr2, payload)
    {:noreply, state}
  end

  @impl GenServer
  def handle_cast({:vote, account1, {{id2, addr2}, account2}=dst, prop}, state) do
    {id, _} = state.me
    Logger.info("#{abbrev(id)}:#{abbrev(account1)}: sending vote to #{abbrev(id2)}:#{abbrev(account2)} of the network")
    src = {state.me, account1}
    msg = ScaleGraph.Consensus.Msg.vote(src, dst, prop)
    payload = encode(msg)
    state.netmod.send(state.net, addr2, payload)
    {:noreply, state}
  end

  @impl GenServer
  def handle_cast({:commit, account1, {{id2, addr2}, account2}=dst, prop}, state) do
    src = {state.me, account1}
    msg = ScaleGraph.Consensus.Msg.commit(src, dst, prop)
    payload = encode(msg)
    state.netmod.send(state.net, addr2, payload)
    {:noreply, state}
  end

  @impl GenServer
  def handle_cast({:process_tx, tx, sig}, state) do
    account = tx.snd
    {instance, state} = _get_instance(state, account)
    Instance.process_tx(instance, {tx, sig})
    {:noreply, state}
  end

  defp _get_instance(state, account) do
    {node_id, _addr} = state.me
    case state.instances[account] do
      nil ->
        # FIXME: instantiate instance via DynamicSupervisor!
        #{:ok, ledger} = Ledgers.ledger(state.ledgers, account)
        ledger =
          case Ledgers.ledger(state.ledgers, account) do
            {:ok, ledger} ->
              ledger
            _ ->
              raise "Consensus #{abbrev(node_id)}: failed to look up instance #{abbrev(account)}"
          end
        opts = [
          node_id: node_id,
          consensus: self(), # FIXME: use a via-name
          ledgers: state.ledgers,
          ledger: ledger,
          config_lookup: state.config_lookup,
        ]
        {:ok, instance} = state.protocol.start_link(opts)
        state = put_in(state.instances[account], instance)
        {instance, state}
      instance ->
        {instance, state}
    end
  end

end


defmodule ScaleGraph.Consensus.Msg do

  # TODO: We don't have to specify the source node ID and address here (we can just already know that here).
  # But we still have to specify the source account ID (i.e. the instance).
  # There is also no need to encode the IP:port into the message sent over the network.
  # It will be in the UDP (or TCP) header anyway.

  def proposal({{id1, addr1}, account1}=src, {{id2, addr2}, account2}=dst, prop) do
    {:consensus, {:propose, src, dst, prop}} # TODO: Signature!
  end

  def vote({{id1, addr1}, account1}=src, {{id2, addr2}, account2}=dst, prop_hash) do
    {:consensus, {:vote, src, dst, prop_hash}} # TODO: Signature!
  end

  def commit({{id1, addr1}, account1}=src, {{id2, addr2}, account2}=dst, prop_hash) do
    {:consensus, {:commit, src, dst, prop_hash}} # TODO: Signature!
  end

  @doc """
  Request information about the last block in the receiver's chain.
  """
  def req_last_block({{id1, addr1}, account1}=src, {{id2, addr2}, account2}=dst) do
    {:consensus, {:req_last_block, src, dst, nil}} # TODO: Signature!
  end

  @doc """
  Reply to a request for last block info.
  Return information about the last block in the receiver's chain.
  """
  def ret_last_block({{id1, addr1}, account1}=src, {{id2, addr2}, account2}=dst, data) do
    {:consensus, {:ret_last_block, src, dst, data}} # TODO: Signature!
  end

  @doc """
  Send a view-change message.
  Must specify `new_view`, e.g. increment current view `v+1`. Lock may be `nil`
  if not locked on any value. Otherwise, `lock` has to be a tuple with
  the locked proposal and a lock certificate.
  """
  def view_change({{id1, addr1}, account1}=src, {{id2, addr2}, account2}=dst, {new_view, lock}=data) do
    {:consensus, {:view_change, src, dst, data}} # TODO: Signature!
  end

  # TODO: What is data? Must include locks from previous views.
  @doc """
  Send a new-view message.
  """
  def new_view({{id1, addr1}, account1}=src, {{id2, addr2}, account2}=dst, data) do
    {:consensus, {:new_view, src, dst, data}} # TODO: Signature!
  end

  @doc """
  Request lock on an account/shard for a given TX and/or other data.
  """
  def req_lock({{id1, addr1}, account1}=src, {{id2, addr2}, account2}=dst, data) do
    {:consensus, {:req_lock, src, dst, data}} # TODO: Signature!
  end

  @doc """
  Reply to a lock request.
  """
  def ret_lock({{id1, addr1}, account1}=src, {{id2, addr2}, account2}=dst, data) do
    {:consensus, {:ret_lock, src, dst, data}} # TODO: Signature!
  end

end
