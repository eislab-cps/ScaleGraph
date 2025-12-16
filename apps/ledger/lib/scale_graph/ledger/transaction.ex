defmodule ScaleGraph.Ledger.Transaction do

  defstruct [
    snd: nil,
    rcv: nil,
    amt: nil,
    nce: nil,
  ]

  def new(sender, receiver, amount, nonce) do
    %__MODULE__{
      snd: sender,
      rcv: receiver,
      amt: amount,
      nce: nonce,
    }
  end

  #@doc "Hash the TX."
  #def hash(tx) do
  #  bin = <<tx.snd::32, tx.rcv::32, tx.amt::8, tx.nce::8>>
  #  Crypto.sha256(bin)
  #end

end

defimpl Hash, for: ScaleGraph.Ledger.Transaction do
  def hash(tx) do
    bin = Bin.to_bin(tx)
    :crypto.hash(:sha256, bin)
  end
end

defimpl Bin, for: ScaleGraph.Ledger.Transaction do
  def to_bin(tx) do
    #<<tx.snd::32, tx.rcv::32, tx.amt::8, tx.nce::8>>
    [tx.snd, tx.rcv, <<tx.amt::8>>, <<tx.nce::8>>]
  end
end

defimpl Abbrev, for: ScaleGraph.Ledger.Transaction do
  def abbrev(tx) do
    "TX{{#{Abbrev.abbrev(tx.snd)}->#{Abbrev.abbrev(tx.rcv)},#{tx.amt},#{tx.nce}}}"
  end
end
