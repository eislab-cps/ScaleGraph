defmodule ScaleGraph.LedgerTest do
  use ExUnit.Case
  alias ScaleGraph.Ledger
  alias Ledger.Account
  alias Ledger.Block
  alias Ledger.Ledgers
  alias Ledger.Transaction
  doctest ScaleGraph.Ledger

  @start_balance 1000

  test "account update" do
    snd_id = 123
    rcv_id = 234
    snd_account = Account.new(snd_id)
      #|> Map.put(:balance, 1000)
    rcv_account = Account.new(rcv_id)
      #|> Map.put(:balance, 1000)
    tx = Transaction.new(snd_id, rcv_id, 100, 0)
    snd_account = Account.update(snd_account, tx)
    rcv_account = Account.update(rcv_account, tx)
    assert snd_account.balance == -100 + @start_balance
    assert snd_account.next_nonce == 1
    assert rcv_account.balance == 100 + @start_balance
    assert rcv_account.next_nonce == 0
  end

  test "ledgers" do
    accountA = Crypto.sha256(123)
    accountB = Crypto.sha256(234)
    {:ok, ledgers} = Ledgers.start_link([])
    Ledgers.create(ledgers, accountA)
    Ledgers.create(ledgers, accountB)
    assert Ledgers.exists?(ledgers, accountA)
    assert Ledgers.exists?(ledgers, accountB)
    assert not Ledgers.exists?(ledgers, Crypto.sha256(321))
  end

  test "last block info" do
    {:ok, ledger} = Ledger.start_link(account_id: 123)
    {index, hash} = Ledger.last_block_info(ledger)
    assert index == 0
    assert hash == <<0::32>>
  end

  # - Start from an empty chain
  # - create a block for TX A -> B (with a null parent block)
  # - commit
  # - create a block for TX A -> B (with a real parent block)
  # - commit
  # - create a block for TX B -> A (with a real parent block)
  test "next block and commit" do
    snd_id = 123
    rcv_id = 234
    {:ok, ledger123} = Ledger.start_link(account_id: snd_id)
    {:ok, ledger234} = Ledger.start_link(account_id: rcv_id)
    # First TX
    tx = Transaction.new(snd_id, rcv_id, 100, 0)
    prev_rcv = Ledger.last_block_info(ledger234)
    header = Ledger.next_block(ledger123, tx, prev_rcv)
    block = Block.new(header, tx, nil)
    # Commit
    Ledger.commit(ledger123, block)
    Ledger.commit(ledger234, block)
    snd_account = Ledger.account(ledger123)
    rcv_account = Ledger.account(ledger234)
    assert snd_account.balance == -100 + @start_balance
    assert snd_account.next_nonce == 1
    assert rcv_account.balance == 100 + @start_balance
    assert rcv_account.next_nonce == 0
    # Second TX
    tx = Transaction.new(snd_id, rcv_id, 200, 0)
    prev_rcv = Ledger.last_block_info(ledger234)
    header = Ledger.next_block(ledger123, tx, prev_rcv)
    block = Block.new(header, tx, nil)
    # Commit
    Ledger.commit(ledger123, block)
    Ledger.commit(ledger234, block)
    snd_account = Ledger.account(ledger123)
    rcv_account = Ledger.account(ledger234)
    assert snd_account.balance == -300 + @start_balance
    assert snd_account.next_nonce == 2
    assert rcv_account.balance == 300 + @start_balance
    assert rcv_account.next_nonce == 0
    # Third TX
    tx = Transaction.new(rcv_id, snd_id, 500, 0)
    prev_rcv = Ledger.last_block_info(ledger123)
    header = Ledger.next_block(ledger234, tx, prev_rcv)
    block = Block.new(header, tx, nil)
    # Commit
    Ledger.commit(ledger123, block)
    Ledger.commit(ledger234, block)
    snd_account = Ledger.account(ledger123)
    rcv_account = Ledger.account(ledger234)
    assert snd_account.balance == 200 + @start_balance
    assert snd_account.next_nonce == 2
    assert rcv_account.balance == -200 + @start_balance
    assert rcv_account.next_nonce == 1
  end

  # - Start from an empty chain
  # - create a block for TX A -> B (with a null parent block)
  # - commit
  # - create a block for TX B -> A (with a real parent block)
  # - commit
  # - create a block for TX A -> B (with a real parent block)
  test "next block and commit 2" do
    snd_id = 123
    rcv_id = 234
    {:ok, ledger123} = Ledger.start_link(account_id: snd_id)
    {:ok, ledger234} = Ledger.start_link(account_id: rcv_id)
    # First TX
    tx = Transaction.new(snd_id, rcv_id, 100, 0)
    prev_rcv = Ledger.last_block_info(ledger234)
    header = Ledger.next_block(ledger123, tx, prev_rcv)
    block = Block.new(header, tx, nil)
    # Commit
    Ledger.commit(ledger123, block)
    Ledger.commit(ledger234, block)
    snd_account = Ledger.account(ledger123)
    rcv_account = Ledger.account(ledger234)
    assert snd_account.balance == -100 + @start_balance
    assert snd_account.next_nonce == 1
    assert rcv_account.balance == 100 + @start_balance
    assert rcv_account.next_nonce == 0
    # Second TX
    tx = Transaction.new(rcv_id, snd_id, 200, 0)
    prev_rcv = Ledger.last_block_info(ledger123)
    header = Ledger.next_block(ledger234, tx, prev_rcv)
    block = Block.new(header, tx, nil)
    # Commit
    Ledger.commit(ledger123, block)
    Ledger.commit(ledger234, block)
    snd_account = Ledger.account(ledger123)
    rcv_account = Ledger.account(ledger234)
    assert snd_account.balance == 100 + @start_balance
    assert snd_account.next_nonce == 1
    assert rcv_account.balance == -100 + @start_balance
    assert rcv_account.next_nonce == 1
    # Third TX
    tx = Transaction.new(snd_id, rcv_id, 500, 0)
    prev_rcv = Ledger.last_block_info(ledger234)
    header = Ledger.next_block(ledger123, tx, prev_rcv)
    block = Block.new(header, tx, nil)
    # Commit
    Ledger.commit(ledger123, block)
    Ledger.commit(ledger234, block)
    snd_account = Ledger.account(ledger123)
    rcv_account = Ledger.account(ledger234)
    assert snd_account.balance == -400 + @start_balance
    assert snd_account.next_nonce == 2
    assert rcv_account.balance == 400 + @start_balance
    assert rcv_account.next_nonce == 1
  end

end
