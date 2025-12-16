defprotocol Bin do

  #@fallback_to_any true

  def to_bin(term)

end

defimpl Bin, for: BitString do
  def to_bin(bin), do: bin
end

#defimpl Bin, for: Any do
#  def to_bin(term) do
#    :erlang.term_to_iovec(term)
#  end
#end
