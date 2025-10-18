defmodule ScaleGraph.DHT.FakeRT do
  @moduledoc """
  "Fake" routing table that stores all contacts (no tree or list of buckets).

  This is a legitimate but impractical routing table. It is the opposite extreme
  of the simple RT (with `b=1`). This RT essentially uses `b = id bits`.

  Since there are no buckets, the `bucket_size` is only relevant as a default
  number of contacts to extract from the RT.
  """
  use GenServer
  alias ScaleGraph.DHT.Contact

  defstruct [
    # Don't need an ID in this RT because there are no buckets.
    bucket_size: nil,
    contacts: %{},
  ]

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, opts)
  end

  @doc """
  Return the the `n` contacts with IDs closest to the target `id`.
  By default, the bucket size is used as the value of `n`.
  """
  def closest(rt, id, n \\ nil) do
    GenServer.call(rt, {:closest, id, n})
  end

  @doc """
  Add a new contact or update an existing contact with new information.
  """
  def update(rt, %Contact{} = contact) do
    GenServer.call(rt, {:update, contact})
  end

  @doc """
  Add a new contact or update an existing contact with new information.
  """
  def update(rt, {id, addr} = _id_addr_pair, rtt \\ nil) do
    contact = Contact.new(id, addr, rtt)
    update(rt, contact)
  end

  @doc """
  Returns the number of contacts stored in the routing table.
  """
  def size(rt) do
    GenServer.call(rt, :size)
  end

  @impl GenServer
  def init(opts) do
    state = %__MODULE__{
      bucket_size: opts[:bucket_size],
    }
    {:ok, state}
  end

  @impl GenServer
  def handle_call({:update, contact}, _caller, state) do
    contact = Contact.update(state.contacts[contact.id], contact)
    state = put_in(state.contacts[contact.id], contact)
    # TODO: return status (added, replaced, updated)
    {:reply, "update status placeholder", state}
  end

  @impl GenServer
  def handle_call({:closest, id, n}, _caller, state) do
    n = n || state.bucket_size
    result = state.contacts
      |> Map.values()
      |> Enum.sort_by(&Util.distance(id, &1.id))
      |> Enum.take(n)
    {:reply, result, state}
  end

  @impl GenServer
  def handle_call(:size, _caller, state) do
    size = map_size(state.contacts)
    {:reply, size, state}
  end

end
