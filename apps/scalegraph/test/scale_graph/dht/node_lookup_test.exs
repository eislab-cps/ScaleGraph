defmodule ScaleGraph.DHT.NodeLookupTest do
  use ExUnit.Case
  alias Netsim.Fake
  alias ScaleGraph.RPC
  alias ScaleGraph.DHT.NodeLookup

  test "responses contain no new candidates" do
    addr1 = {{127, 20, 10, 1}, 1234}
    addr2 = {{127, 20, 10, 2}, 1235}
    addr3 = {{127, 20, 10, 3}, 1236}
    addr4 = {{127, 20, 10, 4}, 1237}
    {:ok, net} = Fake.start_link([name: :fake])
    {:ok, rpc1} = RPC.start_link(net: {Fake, net}, addr: addr1, id: 123, name: :rpc1)
    {:ok, rpc2} = RPC.start_link(net: {Fake, net}, addr: addr2, id: 234, name: :rpc2)
    #
    Fake.connect(net, addr1, rpc1)
    Fake.connect(net, addr2, rpc2)
    Fake.connect(net, addr3, rpc2)
    Fake.connect(net, addr4, rpc2)
    # The initial pool of candidates used when starting lookup.
    candidates = [
      {1235, addr2},
      {1236, addr3},
      {1237, addr4},
    ]
    #
    parent = self()
    #
    spawn(fn ->
      RPC.set_handler(rpc2, self())
      # We expect alpha=2 initial requests.
      assert_receive {:rpc_request, {:find_nodes, _}} = req1
      assert_receive {:rpc_request, {:find_nodes, _}} = req2
      # Lookup is waiting for a response. So, unless we respond to one of
      # them, we won't get any more requests. (Unless perhaps if we wait long
      # enough.)
      assert_no_receive()
      # Respond to one. We suggest the same candidates. These are redundant and
      # will effectively be ignored. Might as well send an empty list.
      RPC.respond(rpc2, req1, candidates)
      # This causes one more new and (final) probe to be sent.
      assert_receive {:rpc_request, {:find_nodes, _}} = req3
      # We have now received the last request. There will be no more requests
      # because there were only 3 candidates. So there can only be 3 probes.
      assert_no_receive()
      # But lookup is not finished, because there are still unanswered requests.
      # Replying to the second request. An empty response is unlikely in
      # practice but not impossible. And handling should be trivial anyway.
      RPC.respond(rpc2, req2, [])
      # Again, this response should not trigger any new requests.
      assert_no_receive()
      # Replying to the last request. We again pass some redundant candidates.
      RPC.respond(rpc2, req3, candidates)
      # Now that all candidates have been exhausted AND all requests have been
      # answered, lookup should finish.
      assert_no_receive(50)
      # Done!
      send(parent, "done 2")
    end)
    #
    spawn(fn ->
      result = NodeLookup.lookup(rpc: rpc1, target: 2345, candidates: candidates, alpha: 2, n_lookup: 3, id: 1234)
      assert length(result.result) == 3
      assert MapSet.new(result.result) == MapSet.new(candidates)
      send(parent, "done 1")
    end)
    #
    assert_receive "done 1", 500
    assert_receive "done 2"
  end


  test "responses contain new candidates" do
    addr1 = {{127, 20, 10, 1}, 1234}
    addr2 = {{127, 20, 10, 2}, 1235}
    addr3 = {{127, 20, 10, 3}, 1236}
    addr4 = {{127, 20, 10, 4}, 1237}
    {:ok, net} = Fake.start_link([name: :fake])
    {:ok, rpc1} = RPC.start_link(net: {Fake, net}, addr: addr1, id: 123, name: :rpc1)
    {:ok, rpc2} = RPC.start_link(net: {Fake, net}, addr: addr2, id: 234, name: :rpc2)
    #
    Fake.connect(net, addr1, rpc1)
    Fake.connect(net, addr2, rpc2)
    Fake.connect(net, addr3, rpc2)
    Fake.connect(net, addr4, rpc2)
    # The initial pool of candidates used when starting lookup.
    candidates = [{1235, addr2}]
    parent = self()
    #
    spawn(fn ->
      RPC.set_handler(rpc2, self())
      # There is only one candidate. So we can only get one request.
      assert_receive {:rpc_request, {:find_nodes, _}} = req1
      assert_no_receive()
      # Now we suggest a new candidate, which will trigger one more probe.
      RPC.respond(rpc2, req1, [{1236, addr3}])
      assert_receive {:rpc_request, {:find_nodes, _}} = req2
      assert_no_receive()
      # Now we suggest a new candidate, which will trigger one more probe.
      # (One of the candidates in the response is redundant!)
      RPC.respond(rpc2, req2, [{1236, addr3}, {1237, addr4}])
      assert_receive {:rpc_request, {:find_nodes, _}} = req3
      assert_no_receive()
      # Now we suggest no new candidates; they are all redundant!
      RPC.respond(rpc2, req3, [{1235, addr2}, {1236, addr3}, {1237, addr4}])
      # So there are no more probes.
      assert_no_receive(50)
      # Done!
      send(parent, "done 2")
    end)
    #
    spawn(fn ->
      result = NodeLookup.lookup(rpc: rpc1, target: 2345, candidates: candidates, alpha: 2, n_lookup: 3, id: 1234)
      assert length(result.result) == 3
      expected = [{1235, addr2}, {1236, addr3}, {1237, addr4}]
      assert MapSet.new(result.result) == MapSet.new(expected)
      send(parent, "done 1")
    end)
    #
    assert_receive "done 1", 500
    assert_receive "done 2"
  end


  test "timeout triggers a new request" do
    addr1 = {{127, 20, 10, 1}, 1234}
    addr2 = {{127, 20, 10, 2}, 1235}
    addr3 = {{127, 20, 10, 3}, 1236}
    addr4 = {{127, 20, 10, 4}, 1237}
    {:ok, net} = Fake.start_link([name: :fake])
    {:ok, rpc1} = RPC.start_link(net: {Fake, net}, addr: addr1, id: 123, name: :rpc1)
    {:ok, rpc2} = RPC.start_link(net: {Fake, net}, addr: addr2, id: 234, name: :rpc2)
    #
    Fake.connect(net, addr1, rpc1)
    Fake.connect(net, addr2, rpc2)
    Fake.connect(net, addr3, rpc2)
    Fake.connect(net, addr4, rpc2)
    # The initial pool of candidates used when starting lookup.
    candidates = [
      {1235, addr2},
      {1236, addr3},
      {1237, addr4},
    ]
    #
    parent = self()
    #
    spawn(fn ->
      RPC.set_handler(rpc2, self())
      assert_receive {:rpc_request, {:find_nodes, _}} = _req1
      assert_receive {:rpc_request, {:find_nodes, _}} = _req2
      assert_no_receive()
      # Refusing to reply. That should eventually trigger a probe of the next
      # candidate.
      assert_receive {:rpc_request, {:find_nodes, _}} = req3
      # Let's respond to this last request so that we can get a result.
      RPC.respond(rpc2, req3, [])
      # There should be no more requests, because there are no more candidates.
      assert_no_receive(50)
      # Done!
      send(parent, "done 2")
    end)
    #
    spawn(fn ->
      result = NodeLookup.lookup(rpc: rpc1, target: 2345, candidates: candidates, alpha: 2, n_lookup: 3, probe_timeout: 100, id: 1234)
      assert length(result.result) == 1
      send(parent, "done 1")
    end)
    #
    assert_receive "done 1", 500
    assert_receive "done 2"
  end


  test "timeout finishes lookup when out of candidates" do
    addr1 = {{127, 20, 10, 1}, 1234}
    addr2 = {{127, 20, 10, 2}, 1235}
    addr3 = {{127, 20, 10, 3}, 1236}
    addr4 = {{127, 20, 10, 4}, 1237}
    {:ok, net} = Fake.start_link([name: :fake])
    {:ok, rpc1} = RPC.start_link(net: {Fake, net}, addr: addr1, id: 123, name: :rpc1)
    {:ok, rpc2} = RPC.start_link(net: {Fake, net}, addr: addr2, id: 234, name: :rpc2)
    #
    Fake.connect(net, addr1, rpc1)
    Fake.connect(net, addr2, rpc2)
    Fake.connect(net, addr3, rpc2)
    Fake.connect(net, addr4, rpc2)
    # The initial pool of candidates used when starting lookup.
    candidates = [{1235, addr2}]
    parent = self()
    #
    spawn(fn ->
      RPC.set_handler(rpc2, self())
      # There is only one candidate. So we can only get one request.
      assert_receive {:rpc_request, {:find_nodes, _}} = req1
      assert_no_receive()
      # Now we suggest a new candidate, which will trigger one more probe.
      RPC.respond(rpc2, req1, [{1236, addr3}])
      assert_receive {:rpc_request, {:find_nodes, _}} = req2
      assert_no_receive()
      # And we respond to that one.
      RPC.respond(rpc2, req2, [{1237, addr4}])
      assert_receive {:rpc_request, {:find_nodes, _}} = _req3
      assert_no_receive(50)
      # But now we don't reply. So, after timeout, lookup is done.
      # Done!
      send(parent, "done 2")
    end)
    #
    spawn(fn ->
      result = NodeLookup.lookup(rpc: rpc1, target: 2345, candidates: candidates, alpha: 2, n_lookup: 3, probe_timeout: 100, id: 1234)
      assert length(result) == 2
      assert MapSet.new(result) == MapSet.new([{1235, addr2}, {1236, addr3}])
      send(parent, "done 1")
    end)
    #
    assert_receive "done 1", 500
    assert_receive "done 2"
  end


  defp assert_no_receive(timeout \\ 10) do
    received = receive do
      msg -> msg
    after timeout -> :nothing
    end
    assert received == :nothing
  end

end
