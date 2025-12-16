defmodule ScaleGraph.Consensus.ZftLock do
  @moduledoc """
  A 0FT (0 fault tolerance) consensus protocol.

  This is essentially an implementation of a fake consensus protocol. It
  correctly serializes concurrent transactions, and it accepts transactions
  in the correct order according to the nonce. However, there is no fault
  tolerance. No node is malicious or crashes. This allows significant
  simplifications. The leader of each shard has complete control over the
  transactions that are proposed for its account. This requires obtaining
  a lock, which is trivial when the leader can singlehandedly grant/reject
  the request.
  """
  use GenServer
  alias ScaleGraph.Consensus
  alias ScaleGraph.Consensus.StaticConfig
  alias ScaleGraph.Ledger
  alias Ledger.Block
  alias Ledger.Transaction
  import Abbrev
  require Logger


  defstruct [
    shard_size: nil,
    # The ID of this node. (Who am I?)
    node_id: nil,
    # The parent consensus module. Can deliver messages etc.
    consensus: nil,
    # Current account state.
    # TODO: maybe it should just be the account ID so that we don't have a
    # cache coherence problem to worry about.
    account: nil,
    # The ledger for this account. Includes the current account state in
    # addition to the actual ledger.
    ledger: nil,
    # Queue of received transactions that we might process at some point.
    tx_queue: [],
    # Where to look up the current shard config. (Could be a DHT that does node
    # lookup, or a config ledger that returns the latest block.)
    config_lookup: nil,
    # Number of votes, etc.
    proposals: %{},
    # This account's/shard's lock. Unlocked/nil or locked to txID.
    shard_lock: nil,
    # Map node IDs to addresses.
    id_to_addr: %{},
    local_shard_ids: nil,
    remote_shard_ids: nil,
    # ---
    proposing: nil,
    receiver_header: nil,
  ]

  # Am I the leader for this shard?
  defp leader?(state) do
    # FIXME: must be based on current view number
    hd(state.local_shard_ids) == state.node_id
  end

  # Am I the leader for this TX?
  defp leader?(state, tx) do
    (tx.snd == state.account.id) and leader?(state)
  end

  defguardp is_leader?(state) when (hd(state.local_shard_ids) == state.node_id)

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, opts)
  end

  def process_tx(instance, {tx, sig}) do
    GenServer.cast(instance, {:process_tx, {tx, sig}})
  end

  # if tx.snd < tx.rcv:
  #   acquire lock(tx.snd, tx)
  #   if successful:
  #     acquire lock(tx.rcv, tx)
  #     if successful:
  #       propose tx
  #     else:
  #       retry later (hold lock on tx.snd)
  #   else:
  #     retry later (holding no lock)
  # else:
  #   acquire lock(tx.rcv, tx)
  #   if successful:
  #     acquire lock(tx.snd, tx)
  #     if successful:
  #       propose tx
  #     else:
  #       retry later (hold lock on tx.rcv)
  #   else:
  #     retry later (holding no lock)

  defp start_propose_tx(state, {tx, _sig}) do
    state = Map.put(state, :proposing, tx)
    state = lookup_remote_shard(state, tx)
    acquire_lock_1(state)
  end

  defp lock_1_acquired?(state) do
    tx = state.proposing
    if tx.snd < tx.rcv do
      state.shard_lock == tx
    else
      not is_nil(state.receiver_header)
    end
  end

  defp lock_2_acquired?(state) do
    tx = state.proposing
    if tx.snd < tx.rcv do
      not is_nil(state.receiver_header)
    else
      state.shard_lock == tx
    end
  end

  defp acquire_lock_1(state) do
    tx = state.proposing
    cond do
      tx.rcv < tx.snd ->
        receiver_lock(state, tx)
      is_nil(state.shard_lock) ->
        state = local_lock(state, tx)
        continue_propose(state)
      :else ->
        state
    end
  end

  defp acquire_lock_2(state) do
    tx = state.proposing
    cond do
      tx.snd < tx.rcv ->
        receiver_lock(state, tx)
      is_nil(state.shard_lock) ->
        state = local_lock(state, tx)
        continue_propose(state)
      :else ->
        state
    end
  end

  defp continue_propose(state) do
    if is_nil(state.proposing) do
      fail(state, "cannot propose: no pending TX")
    end
    cond do
      is_nil(state.proposing) ->
        error(state, "aborting proposal!")
        state
      lock_1_acquired?(state) and lock_2_acquired?(state) ->
        propose_tx(state)
      lock_1_acquired?(state) ->
        acquire_lock_2(state)
      :else ->
        acquire_lock_1(state)
    end
  end

  defp propose_tx(state) do
    cond do
      is_nil(state.proposing) ->
        fail(state, "cannot propose: no pending TX")
      is_nil(state.shard_lock) ->
        fail(state, "cannot propose: has not locked self")
      is_nil(state.receiver_header) ->
        fail(state, "cannot propose: has not locked receiver (no header)")
      not lock_1_acquired?(state) ->
        fail(state, "cannot propose: lock 1 not aquired!")
      not lock_2_acquired?(state) ->
        fail(state, "cannot propose: lock 2 not aquired!")
      :else ->
        nil
    end
    tx = state.proposing
    proposal = build_proposal(state)
    info(state, "Broadcasting PROPOSE...")
    Enum.each(state.local_shard_ids, fn id ->
      if id != state.node_id do
        addr = Map.fetch!(state.id_to_addr, id)
        #info(state, "Sending PROPOSE to #{abbrev(id)},#{inspect(addr)}")
        Consensus.propose(state.consensus, state.account.id, {{id, addr}, tx.snd}, proposal)
      end
    end)
    Enum.each(state.remote_shard_ids, fn id ->
      addr = Map.fetch!(state.id_to_addr, id)
      #info(state, "Sending PROPOSE to #{abbrev(id)},#{inspect(addr)}")
      Consensus.propose(state.consensus, state.account.id, {{id, addr}, tx.rcv}, proposal)
    end)
    state = record_commit(state, proposal)
    Map.put(state, :state, :busy)
  end

  defp build_proposal(state) do
    tx = state.proposing
    header = Ledger.next_block(state.ledger, tx, state.receiver_header)
    proposal = Block.new(header, tx, nil)
    proposal
  end

  # For now, the leader unilaterally manages the lock. So acquiring the lock is
  # trivial. In the BFT case, we'll have to run intra-shard consensus.
  defp local_lock(state, %Transaction{}=tx) do
    Map.put(state, :shard_lock, tx)
  end

  defp receiver_lock(state, %Transaction{}=tx) do
    peer_id = hd(state.remote_shard_ids)
    peer_addr = state.id_to_addr[peer_id]
    peer = {peer_id, peer_addr}
    info(state, "requesting lock")
    Consensus.request_receiver_lock_async(state.consensus, state.account.id, {peer, tx.rcv}, tx)
    state
  end

  @impl GenServer
  def init(opts) do
    ledger = Keyword.fetch!(opts, :ledger)
    account = Ledger.account(ledger)
    config_lookup = Keyword.fetch!(opts, :config_lookup)
    local_shard = StaticConfig.get(config_lookup, account.id)

    local_ids = Enum.map(local_shard, fn {id, _addr} -> id end)
    id_to_addr = Map.new(local_shard)
    # FIXME: Shard size should not be optional!
    # (the shard size should determine the shard config, not the other way around!)
    shard_size = Keyword.get(opts, :shard_size, length(local_shard))

    state = %__MODULE__{
      node_id: Keyword.fetch!(opts, :node_id),
      consensus: Keyword.fetch!(opts, :consensus),
      ledger: ledger,
      account: account,
      config_lookup: config_lookup,
      shard_size: shard_size,
      local_shard_ids: local_ids,
      id_to_addr: id_to_addr,
    }
    info(state, "starting")
    {:ok, state}
  end

  # If tx.nonce < account.next_nonce: reject
  # If tx.nonce > account.next_nonce: enqueue and wait
  # (else tx.nonce = account.next_nonce)
  # If leader?: start round (make proposal)
  # If not leader?: enqueue and wait for proposal
  # TODO: maybe this should be a call so we can return some status and/or ID?
  @impl GenServer
  #def handle_cast({:process_tx, {tx, sig}}, %{state: :idle}=state) do
  def handle_cast({:process_tx, {tx, sig}}, %{proposing: nil}=state) do
    info(state, "Consensus instance idle, processing TX: #{abbrev(tx)}")
    {tx_stat, reason} = tx_status(state, {tx, sig})
    leader? = leader?(state, tx)
    case tx_stat do
      :invalid ->
        error(state, "TX is invalid (#{reason}), discarding")
        {:noreply, state}
      :wait ->
        warning(state, "TX is not currently valid (#{reason}), enqueueing")
        state = enqueue_tx(state, {tx, sig})
        {:noreply, state}
      :valid ->
        info(state, "TX is valid")
        state =
          if leader? do
            info(state, "I am leader, proposing TX...")
            start_propose_tx(state, {tx, sig})
          else
            info(state, "not leader, enqueueing and waiting for proposal")
            q = [{tx, sig} | state.tx_queue] # FIXME: must sort by nonce!
            state = Map.put(state, :tx_queue, q)
            state
          end
        {:noreply, state}
    end
  end

  # Hmm, maybe attaching the signature to the TX does make more sense after all.
  # TODO: maybe this should be a call so we can return some status and/or ID?
  @impl GenServer
  def handle_cast({:process_tx, {tx, sig}}, state) do
    warning(state, "Consensus instance busy, enqueing TX")
    q = [{tx, sig} | state.tx_queue] # FIXME: must sort by nonce!
    state = Map.put(state, :tx_queue, q)
    {:noreply, state}
  end

  defp lookup_remote_shard(state, %Transaction{}=tx) do
    account_id =
      cond do
        tx.snd == state.account.id -> tx.rcv
        tx.rcv == state.account.id -> tx.snd
        :else -> raise "wtf!"
      end
    remote = StaticConfig.get(state.config_lookup, account_id)
    remote_ids = Enum.map(remote, fn {id, _addr} -> id end)
    id_to_addr = Map.merge(state.id_to_addr, Map.new(remote))
    %{state |
      remote_shard_ids: remote_ids,
      id_to_addr: id_to_addr,
    }
  end

  defp lookup_remote_shard(state, %Block{}=proposal) do
    tx = proposed_tx(state, proposal)
    lookup_remote_shard(state, tx)
  end

  @impl GenServer
  def handle_cast({:req_lock, {{id, _addr}, src_account}=from, tx}, %{shard_lock: nil}=state) do
    info(state, "Got LOCK REQUEST from #{instance_addr(id, src_account)} for #{abbrev(tx)}")
    header_info = Ledger.last_block_info(state.ledger)
    info(state, "GRANTING lock and returning last block info")
    Consensus.answer_lock_request(state.consensus, state.account.id, from, {:granted, header_info})
    state = Map.put(state, :shard_lock, tx)
    info(state, "done answering, waiting for PROPOSE")
    {:noreply, state}
  end

  # TODO
  # If the requester blocks until the lock can be granted, that greatly
  # simplfies things, at least in the 0FT case, because then there's no need to
  # schedule a retry. Locking always succeeds (eventually).
  @impl GenServer
  def handle_cast({:req_lock, {{id, _addr}, src_account}=from, tx}, state) do
    info(state, "Got LOCK REQUEST from #{instance_addr(id, src_account)} for #{abbrev(tx)}")
    info(state, "REFUSING lock because the shard is already locked!")
    # TODO: enqueue the request and grant the lock after commit.
    Consensus.answer_lock_request(state.consensus, state.account.id, from, {:denied, nil})
    {:noreply, state}
  end

  # Got the reply to a lock request we sent earlier.
  @impl GenServer
  def handle_cast({:ret_lock, _from, data}, state) do
    state =
      case data do
        {:granted, header_info} ->
          state = Map.put(state, :receiver_header, header_info)
          # Either we have already locked local, or we'll do it now.
          if is_nil(state.proposing) or not is_nil(state.receiver_header) do
            error(state, "did I ask for this lock!?") # FIXME: This is weird
            continue_propose(state)
          else
            warning(state, "lock request granted, continuing with proposal")
            continue_propose(state)
          end
        {:denied, nil} ->
          # Ask again
          if is_nil(state.proposing) or not is_nil(state.receiver_header) do
            error(state, "lock request denied. Probably redundant. Aborting!")
            state
          else
            warning(state, "lock request denied, requesting again")
            receiver_lock(state, state.proposing)
          end
          state
      end
    {:noreply, state}
  end

  # NOTE: Only followers receive PROPOSE messages.
  # 0FT: Everyone is "honest". So, just accept the proposal as valid and commit!
  @impl GenServer
  def handle_cast({:propose, from, %Block{}=proposal}, state) do
    #warning(state, "Got a PROPOSAL from #{instance_addr(id, src_account)}")
    warning(state, "Got a PROPOSAL")
    state = add_proposal(state, proposal)
    state = commit(state, proposal)
    Consensus.commit(state.consensus, state.account.id, from, proposal)
    {:noreply, state}
  end

  # NOTE: Only the leader (proposer) receives COMMIT messages.
  @impl GenServer
  def handle_cast({:commit, {{id, _addr}, src_account}=_from, %Block{}=proposal}, state) do
    info(state, "Got a COMMIT from #{abbrev(id)}:#{abbrev(src_account)}.")
    state = record_commit(state, proposal, src_account, id)
    state =
      cond do
        should_commit?(state, proposal) ->
          commit(state, proposal)
        :else ->
          state
      end
    {:noreply, state}
  end

  defp record_commit(state, proposal, voter_shard \\ nil, voter_id \\ nil) do
    state = add_proposal(state, proposal)
    voter_shard = voter_shard || state.account.id
    voter_id = voter_id || state.node_id
    tx = proposed_tx(state, proposal)
    cond do
      voter_shard == tx.snd ->
        info(state, "recorded COMMIT from sender shard")
        snd_commits = MapSet.put(state.proposals[proposal].snd_commits, voter_id)
        put_in(state.proposals[proposal].snd_commits, snd_commits)
      voter_shard == tx.rcv ->
        info(state, "recorded COMMIT from receiver shard")
        rcv_commits = MapSet.put(state.proposals[proposal].rcv_commits, voter_id)
        put_in(state.proposals[proposal].rcv_commits, rcv_commits)
      :else ->
        error(state, "account does not match!")
        state
    end
  end

  defp should_commit?(state, proposal) do
    #(not state.proposals[proposal].committed?
    #  and sufficient_commits?(state, proposal))
    sufficient_commits?(state, proposal)
  end

  # May have to continue with a pending proposal that was blocked when acquiring locks.
  # I always free my local shard/account lock!
  # If the committed proposal was my proposal, I'm done.
  # If not, I'm still working on my proposal and need to continue.
  defp commit(state, %Block{}=proposal) do # when is_leader?(state) do
    warn(state, "Committing!")
    tx = proposed_tx(state, proposal)
    {:ok, account} = Ledger.commit(state.ledger, proposal)
    state = Map.put(state, :account, account)
    state = put_in(state.proposals[proposal].committed?, true)
    info(state, "Account after commit: #{abbrev(state.account)}")
    # Reset state.
    state = reset(state)
    # We're done now. TX has been committed. If more TXs were enqueued, we
    # should start processing the next one.
    if leader?(state) do
      if (not is_nil(state.proposing)) and (state.proposing != tx) do
        warning(state, "committed a foregin TX, continuing with my TX")
        continue_propose(state)
      else
        state = Map.put(state, :proposing, nil)
        case pop_next_tx(state) do
          {nil, state} ->
            warning(state, "Committed TX. Queue empty. Done.")
            state
          {{tx, sig}, state} ->
            warning(state, "Committed TX, starting the next round with enqueued #{abbrev(tx)}")
            start_propose_tx(state, {tx, sig})
        end
      end
    else
      state
    end
  end

  defp enqueue_tx(state, tx_sig) do
    q = [tx_sig | state.tx_queue]
    Map.put(state, :tx_queue, q)
  end

  # Dequeue the next valid TX, if one exists.
  # Returns {nil, state} or {tx_sig, state'}.
  defp pop_next_tx(state) do
    index =
      Enum.find_index(state.tx_queue, fn {tx, _sig} ->
        (tx.nce == state.account.next_nonce
          and tx.snd == state.account.id
          and tx.amt <= state.account.balance)
          # FIXME: check signature
      end)
    if is_nil(index) do
      {nil, state}
    else
      tx_sig = Enum.at(state.tx_queue, index)
      q = List.delete_at(state.tx_queue, index)
      state = Map.put(state, :tx_queue, q)
      {tx_sig, state}
    end
  end

  defp reset(state) do
    %__MODULE__{state |
      shard_lock: nil,
      receiver_header: nil,
    }
  end

  # Currently simple, since we are proposing a full block, including the body
  # and its TX. In the future, we may have to find the TX in the mempool by
  # its hash.
  defp proposed_tx(_state, %Block{}=proposal) do
    block = proposal
    block.body.tx
  end

  # The TX is not just valid or invalid.
  # It may be valid right now. Or it may be invalid right now.
  # Or it can be potentially valid in the future depending on its nonce.
  defp tx_status(state, {tx, _sig}) do
    cond do
      tx.snd == tx.rcv ->
        # FIXME: Need to figure out how to handle this edge case.
        {:invalid, "FIXME: A->A not allowed"}
      (state.account.id == tx.snd) and (tx.nce < state.account.next_nonce) ->
        # Definitely invalid due to nonce.
        {:invalid, "nonce < next nonce"}
      (state.account.id == tx.snd) and (tx.nce > state.account.next_nonce) ->
        # Potentially valid in the future, but not right now.
        {:wait, "nonce > next nonce"}
      (state.account.id == tx.snd) and (state.account.balance < tx.amt) ->
        {:invalid, "balance #{inspect(state.account.balance)} < amount #{inspect(tx.amt)}"}
      :else ->
        # FIXME: Signature valid?
        {:valid, "OK"}
    end
  end

  defp add_proposal(state, proposal) do
    if state.proposals[proposal] do
      state
    else
      prop_stats = %{
        snd_votes: MapSet.new(),
        rcv_votes: MapSet.new(),
        snd_commits: MapSet.new(),
        rcv_commits: MapSet.new(),
        locked?: false,
        committed?: false,
      }
      put_in(state.proposals[proposal], prop_stats)
    end
  end

  defp sufficient_commits?(state, proposal \\ nil) do
    proposal = proposal || state.locked_on
    required = state.shard_size
    snd_commits_needed = required - MapSet.size(state.proposals[proposal].snd_commits)
    rcv_commits_needed = required - MapSet.size(state.proposals[proposal].rcv_commits)
    cond do
      (snd_commits_needed > 0) and (rcv_commits_needed > 0) ->
        info(state, "Need #{snd_commits_needed} more commits from sender side and #{rcv_commits_needed} from receiver side")
        false
      snd_commits_needed > 0 ->
        info(state, "Need #{snd_commits_needed} more commits from sender side")
        false
      rcv_commits_needed > 0 ->
        info(state, "Need #{rcv_commits_needed} more commits from receiver side")
        false
      :else ->
        true
    end
  end

  defp fail(state, msg) do
    raise "#{abbrev(state.node_id)}:#{abbrev(state.account.id)}: #{msg}"
  end

  defp instance_addr(node_id, account_id) do
    "#{abbrev(node_id)}:#{abbrev(account_id)}"
  end

  defp _abbrev(id) do
    result = abbrev(id)
    # FOR DEBUGGING
    case result do
      "0x5607…B06B" -> "A"
      "0xBC88…6763" -> "B"
      "0x5753…09E6" -> "C"
      "0x149B…25DF" -> "N1"
      "0xB410…60D3" -> "N2"
      "0x4B6C…C383" -> "N3"
      "0x2DE5…3172" -> "N4"
      _ -> result
    end
  end

  defp info(state, s) do
    #Logger.info("#{_abbrev(state.node_id)}:#{_abbrev(state.account.id)}: #{s}")
    state
  end

  defp warn(state, s) do
    #Logger.warning("#{_abbrev(state.node_id)}:#{_abbrev(state.account.id)}: #{s}")
    state
  end

  defp warning(state, s) do
    #Logger.warning("#{_abbrev(state.node_id)}:#{_abbrev(state.account.id)}: #{s}")
    state
  end

  defp error(state, s) do
    #Logger.error("#{_abbrev(state.node_id)}:#{_abbrev(state.account.id)}: #{s}")
    state
  end

  defp check_invariants(state) do
    state # FIXME: Do some actual invariants check!
  end

end
