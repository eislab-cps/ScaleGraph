defmodule Crypto do
  @moduledoc """
  Easy access to cryptographic functions, especially ED25519.

  This module wraps the Erlang `:crypto` module and provides more convenient
  access specifically to ED25519 operations for signing and verification.

  ## Examples

      # Signing the whole message (not its hash), passing key pairs.
      iex> keys = Crypto.generate_keys()
      iex> msg = {"hello", [:world]}
      iex> sig = Crypto.sign(msg, keys)
      iex> Crypto.valid?(sig, msg, keys)
      true

      # Signing the message hash, passing priv/public keys.
      iex> keys = Crypto.generate_keys()
      iex> msg = {"hello", [:world]}
      iex> sig = Crypto.sign(msg, keys.priv, :hash) # :hash is truthy
      iex> Crypto.valid?(sig, msg, keys.pub, :hash)
      true

      # Signing the message hash but verifying the whole message (mismatch!)
      iex> keys = Crypto.generate_keys()
      iex> msg = {"hello", [:world]}
      iex> sig = Crypto.sign(msg, keys.priv, :hash)
      iex> Crypto.valid?(sig, msg, keys.pub)
      false
  """

  @doc """
  Generates an ED25519 public/private key pair, returned as a
  `%{pub: public_key, priv: private_key}` map.
  """
  def generate_keys() do
    {pub, priv} = :crypto.generate_key(:eddsa, :ed25519)
    %{pub: pub, priv: priv}
  end

  @doc """
  Generates an ED25519 signature for `msg` using private key `priv`.

  The message can be an arbitrary term or a binary. It will be converted to a
  binary if necessary. `priv` can be a (binary) private key or a key pair
  (i.e. a map with a `:priv` key).
  If `hash?` is set to `true`, the SHA256 hash of `msg` is signed.
  If `false` (the default) the whole message is signed.
  """
  def sign(msg, priv, hash? \\ false)

  def sign(msg, priv, hash?) when is_binary(priv) do
    msg =
      cond do
        hash? -> sha256(msg)
        :else -> term_to_bin(msg)
      end

    :crypto.sign(:eddsa, :none, msg, [priv, :ed25519])
  end

  def sign(msg, %{priv: priv}, hash?) do
    sign(msg, priv, hash?)
  end

  @doc """
  Use public key `pub` to check whether `sig` is a valid signature for
  message `msg`.

  The message can be an arbitrary term or a binary. It will be converted to a
  binary if necessary. `pub` can be a (binary) public key or a key pair
  (i.e. a map with a `:pub` key).
  If `hash?` is `true`, the signature is checked for the SHA256
  hash of `msg` rather than the whole message (the default).
  """
  def valid?(sig, msg, pub, hash? \\ false)

  def valid?(sig, msg, pub, hash?) when is_binary(pub) do
    msg =
      cond do
        hash? -> sha256(msg)
        :else -> term_to_bin(msg)
      end

    :crypto.verify(:eddsa, :none, msg, sig, [pub, :ed25519])
  end

  def valid?(sig, msg, %{pub: pub}, hash?) do
    valid?(sig, msg, pub, hash?)
  end

  @doc """
  Compute the SHA256 hash of `msg`, which can be an arbitrary term.
  """
  def sha256(msg) do
    :crypto.hash(:sha256, term_to_bin(msg))
    #:crypto.hash(:sha256, Bin.to_bin(msg))
  end

  def hash(msg) do
    bin = Bin.to_bin(msg) |> IO.iodata_to_binary()
    :crypto.hash(:sha256, bin)
  end

  defp term_to_bin(term) when is_bitstring(term), do: term

  defp term_to_bin(term), do: :erlang.term_to_binary(term)
end
