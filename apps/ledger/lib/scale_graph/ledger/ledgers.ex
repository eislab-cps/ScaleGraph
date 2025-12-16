defmodule ScaleGraph.Ledger.Ledgers do
  @moduledoc """
  Represents the collection of all ledgers/accounts.
  """
  alias ScaleGraph.Ledger
  use GenServer

  # TODO: Will need some way to separate "ledger exists for an account with ID"
  # and "account with ID has not yet been deleted/inactivated".
  defstruct [
    ledgers: %{},
  ]

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, opts)
  end

  # TODO: what if it already exists?
  @doc """
  Create a new account/ledger with ID `account_id`.
  """
  def create(ledgers, account_id) do
    GenServer.call(ledgers, {:create, account_id})
  end

  @doc """
  Fetch the ledger for a given `account_id`. The result may be a PID or name,
  or `:noaccount` if the account does not exist.
  """
  def ledger(ledgers, account_id) do
    GenServer.call(ledgers, {:ledger, account_id})
  end

  # TODO: exists vs exists (created vs deleted).
  @doc """
  Check if an account with ID `account_id` exists.
  """
  def exists?(ledgers, account_id) do
    GenServer.call(ledgers, {:exists?, account_id})
  end

  @impl GenServer
  def init(_opts) do
    state = %__MODULE__{}
    {:ok, state}
  end

  @impl GenServer
  def handle_call({:create, account_id}, _caller, state) do
    if Map.has_key?(state.ledgers, account_id) do
      {:reply, state.ledgers[account_id], state}
    else
      {:ok, ledger} = Ledger.start_link(account_id: account_id)
      state = put_in(state.ledgers[account_id], ledger)
      {:reply, ledger, state}
    end
  end

  @impl GenServer
  def handle_call({:ledger, account_id}, _caller, state) do
    result =
      case state.ledgers[account_id] do
        nil ->
          {:noaccount, nil}
        ledger ->
          {:ok, ledger}
      end
    {:reply, result, state}
  end

  @impl GenServer
  def handle_call({:exists?, account_id}, _caller, state) do
    result = Map.has_key?(state.ledgers, account_id)
    {:reply, result, state}
  end

end
