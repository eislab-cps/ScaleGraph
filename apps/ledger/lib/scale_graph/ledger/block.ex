defmodule ScaleGraph.Ledger.Block do
  @moduledoc """
  A block containing one transaction.

  A block has a header and a body. The body is tiny.

  ## Index / Height

  The "index" of a block (often called its "height") is the 1-based index of
  the block. That is, the first block has index/height 1 and is the head of a
  chain of length 0. (The previous block has an index/height of 0.)
  """

  defmodule Header do
    defstruct [
      prev_index_snd: 0,
      prev_index_rcv: 0,
      prev_hash_snd: <<0::32>>,
      prev_hash_rcv: <<0::32>>,
      tx_id: nil,
      # Epochs are not used right now.
      # Epoch number is the number of the current epoch.
      # Epoch cfg is the hash of the current epoch block.
      #epoch_num_snd: 0,
      #epoch_cfg_snd: nil,
      #epoch_num_rcv: 0,
      #epoch_cfg_rcv: nil,
    ]
  end

  defmodule Body do
    defstruct [
      tx: nil,
      tx_sig: nil,
      # TODO: more info here? Maybe a quorum cert?
    ]
  end

  defstruct [
    header: nil,
    body: nil,
  ]

  def new(header, tx, tx_sig) do
    %__MODULE__{
      header: header,
      body: %Body{
        tx: tx,
        tx_sig: tx_sig,
      }
    }
  end

  #@doc "Hash the block header."
  #def hash(block_header)

  #def hash(%Block{} = block) do
  #  hash(block.header)
  #end

end

defimpl Bin, for: ScaleGraph.Ledger.Block.Header do
  def to_bin(header) do
    idx_s = header.prev_index_snd
    idx_r = header.prev_index_rcv
    hash_s = header.prev_hash_snd
    hash_r = header.prev_hash_rcv
    tx_id = header.tx_id
    [<<idx_s::8>>, <<idx_r::8>>, hash_s, hash_r, tx_id]
  end
end
