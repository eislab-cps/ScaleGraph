defmodule ScaleGraph.Ledger.Account do
  @moduledoc """
  An account is an abstraction of the ledger state.

  It summarizes the current state of the account, derived from the ledger.
  """

  defstruct [
    id: nil,
    balance: 1000,
    next_nonce: 0,
    prev_tx_hash: nil,
  ]

  def new(account_id) do
    %__MODULE__{
      id: account_id,
    }
  end

  # TODO: These validations should exist somewhere else!
  # Otherwise, we might commit the block but then refuse to update the account!
  def update(account, tx) do
    hash = Crypto.hash(tx)
    cond do
      tx.snd == tx.rcv ->
        # This should never happen!
        raise "TX #{inspect(tx.snd)} -> #{inspect(tx.rcv)} has the same sender and receiver"
      account.id == tx.snd ->
        balance = account.balance - tx.amt
        nonce = account.next_nonce + 1
        %__MODULE__{account | prev_tx_hash: hash, balance: balance, next_nonce: nonce}
      account.id == tx.rcv ->
        balance = account.balance + tx.amt
        %__MODULE__{account | prev_tx_hash: hash, balance: balance}
      :else ->
        # This should never happen!
        raise "TX #{inspect(tx.snd)} -> #{inspect(tx.rcv)} does not involve account #{inspect(account.id)}"
    end
  end

end

defimpl Abbrev, for: ScaleGraph.Ledger.Account do
  def abbrev(account) do
    "Account{{id: #{Abbrev.abbrev(account.id)}, balance: #{account.balance}, next_nonce: #{account.next_nonce}}}"
  end
end
