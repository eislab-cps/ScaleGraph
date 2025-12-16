defprotocol Abbrev do

  def abbrev(term)

end

defimpl Abbrev, for: BitString do

  def abbrev(bin) when byte_size(bin) > 4 do
    prefix = bin
             |> :binary.part(0, 2)
             |> :binary.decode_unsigned()
             |> Integer.to_string(16)
             |> String.pad_leading(4, "0")
    suffix = bin
             |> :binary.part(byte_size(bin)-2, 2)
             |> :binary.decode_unsigned()
             |> Integer.to_string(16)
             |> String.pad_leading(4, "0")
    "0x#{prefix}…#{suffix}"
  end

  def abbrev(bin) do
    first = :binary.first(bin)
    s = :binary.decode_unsigned(bin)
        |> Integer.to_string(16)
    if first < 16 do
      "0x0#{s}"
    else
      "0x#{s}"
    end
  end

end
