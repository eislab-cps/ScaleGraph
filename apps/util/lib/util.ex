defmodule Util do
  @moduledoc """
  Utility functions for working with IDs/keys, generating random numbers, etc.
  """

  @doc """
  Convert a (binary) cryptographic key to a node ID (integer).

  ## Examples

      iex> Util.key_to_id(<<255>>)
      255
      iex> Util.key_to_id(<<1, 0>>)
      256
      iex> Util.key_to_id(<<7, 91, 205, 21>>)
      123456789
  """
  def key_to_id(key), do: bin_to_int(key)

  @doc """
  Convert a node ID (integer) to a (binary) cryptographic key.

  ## Examples

      iex> Util.id_to_key(255)
      <<255>>
      iex> Util.id_to_key(256)
      <<1, 0>>
      iex> Util.id_to_key(123456789)
      <<7, 91, 205, 21>>
  """
  def id_to_key(id), do: int_to_bin(id)

  @doc """
  Return the XOR-distance between `id1` and `id2`, which may be either
  integer IDs or binary keys.

  ## Examples

      iex> Util.distance(12345, 12345)
      0
      iex> Util.distance(0b001101, 0b001011)
      6 # 0b000110
      iex> Util.distance(<<2, 211>>, <<5, 108>>) # 0b10_11010011, 0b101_01101100
      1983 # <<7, 191>>
  """
  def distance(id1, id2) do
    Bitwise.bxor(key_to_id(id1), key_to_id(id2))
  end

  @doc """
  Return the longest shared prefix of `id1` and `id2`, given that they are
  both `bits`-bit integers.

  ## Examples

      iex> Util.prefix_len(0b01, 0b10, 2)
      0
      iex> Util.prefix_len(0b1011, 0b0011, 4)
      0
      iex> Util.prefix_len(0b0011, 0b0010, 4)
      3
      iex> Util.prefix_len(0b0011, 0b0011, 4)
      4
      iex> Util.prefix_len(0, 1, 256)
      255
      iex> Util.prefix_len(12345, 12345, 16)
      16
  """
  def prefix_len(id1, id2, bits)

  # Need to treat id1 == id2 as a special case because we can't do log(0).
  def prefix_len(id, id, bits), do: bits

  def prefix_len(id1, id2, bits) do
    xor = distance(id1, id2)
    bits - (trunc(:math.log2(xor)) + 1)
  end

  # Convert a binary to integer, or leave unchanged if already an integer.
  defp bin_to_int(x) when is_integer(x), do: x
  defp bin_to_int(x) when is_binary(x), do: :binary.decode_unsigned(x)

  # Convert an integer to binary, or leave unchanged if already a binary.
  defp int_to_bin(x) when is_binary(x), do: x
  defp int_to_bin(x) when is_integer(x), do: :binary.encode_unsigned(x)

  @doc """
  Generate a (uniformly) random integer in the range `[0, max]` (inclusive).

  ## Examples

      iex> Util.rand(10) in 0..10
      true
      iex> Util.rand(1) in 0..1
      true
      iex> Util.rand(0)
      0
  """
  def rand(max), do: rand(0, max)

  @doc """
  Generate a (uniformly) random integer in the range `[min, max]` (inclusive).

  ## Examples

      iex> Util.rand(5, 10) in 5..10
      true
      iex> Util.rand(10, 10)
      10
      iex> Util.rand(0, 0)
      0
      iex> Util.rand(-7, -7)
      -7
  """
  def rand(min, max), do: :rand.uniform(max - min + 1) + min - 1

  @doc """
  Generate a (uniformly) random `n`-bit number.

  ## Examples

      iex> Util.rand_bits(5) in 0..0b11111
      true
      iex> Util.rand_bits(1) in 0..1
      true
  """
  def rand_bits(n) do
    max = Bitwise.<<<(1, n) - 1
    rand(0, max)
  end
end
