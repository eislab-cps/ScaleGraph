defmodule ScaleGraph.Ledger do
  @moduledoc """
  Documentation for `Ledger`.
  """
  alias ScaleGraph.Ledger.Account
  use GenServer

  defstruct [
    # TODO: maintain a log file
    blocks: [],
    account: nil,
    # Keep track of which TXs have been committed in the past.
    past_txs: nil,
  ]

  # TODO: Needs an account ID and/or file
  def start_link(opts) do
    {account_id, opts} = Keyword.pop!(opts, :account_id)
    GenServer.start_link(__MODULE__, {account_id}, opts)
  end

  def account(ledger) do
    GenServer.call(ledger, :get_account)
  end

  @doc """
  Get the index and hash of the last block in the chain.
  """
  def last_block_info(ledger) do
    GenServer.call(ledger, :last_block_info)
  end

  @doc """
  Build the block header of the next block for the transaction `tx` relative to
  the sender account's (i.e. this) ledger. `prev_rcv` is the `{index, hash}` of
  the previous (last) block in the receiver account's chain.
  """
  def next_block(ledger, tx, {_rcv_index, _rcv_hash}=prev_rcv) do
    GenServer.call(ledger, {:next_block, tx, prev_rcv})
  end

  @doc """
  Commit the new block to the ledger.
  Returns `{:ok, account}` on success, where `account` is the new, updated
  account state.
  """
  # TODO: take a complete block, or just a header + {TX, sig}?
  def commit(ledger, block) do
    GenServer.call(ledger, {:commit, block})
  end

  defp _last_block_info(state) do
    case state.blocks do
      [] ->
        {0, <<0::32>>}
      [block | _] ->
        #index = length(state.blocks)
        index = block.header.prev_index_snd + 1
        hash = Crypto.hash(block.header)
        {index, hash}
    end
  end

  @impl GenServer
  def init({account_id}) do
    state = %__MODULE__{
      blocks: [],
      account: Account.new(account_id),
      past_txs: MapSet.new(),
    }
    {:ok, state}
  end

  @impl GenServer
  def handle_call(:get_account, _caller, state) do
    result = state.account
    {:reply, result, state}
  end

  @impl GenServer
  def handle_call(:last_block_info, _caller, state) do
    result = _last_block_info(state)
    {:reply, result, state}
  end

  @impl GenServer
  def handle_call({:next_block, tx, {rcv_index, rcv_hash}}, _caller, state) do
    hash = Crypto.hash(tx)
    {snd_index, snd_hash} = _last_block_info(state)
    header = %ScaleGraph.Ledger.Block.Header{
      prev_hash_snd: snd_hash,
      prev_hash_rcv: rcv_hash,
      prev_index_snd: snd_index,
      prev_index_rcv: rcv_index,
      tx_id: hash,
    }
    {:reply, header, state}
  end

  # TODO: take a complete block, or just a header + {TX, sig}?
  @impl GenServer
  def handle_call({:commit, block}, _caller, state) do
    blocks = [block | state.blocks]
    account = Account.update(state.account, block.body.tx)
    past_txs = MapSet.put(state.past_txs, block.header.tx_id)
    state = %__MODULE__{
      blocks: blocks,
      account: account,
      past_txs: past_txs,
    }
    {:reply, {:ok, account}, state}
  end

  #@impl GenServer
  #def handle_call(:last_block_header, _caller, state) do
  #  [block | _] = state.blocks
  #  result = {block.header, state.account}
  #  {:reply, result, state}
  #end

end
