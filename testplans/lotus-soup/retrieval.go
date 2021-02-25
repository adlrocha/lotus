package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"strconv"
	"time"

	"github.com/filecoin-project/lotus/api"
	"github.com/testground/sdk-go/sync"

	"github.com/filecoin-project/lotus/testplans/lotus-soup/testkit"
)

func retrieval(t *testkit.TestEnvironment) error {
	// Dispatch/forward non-client roles to defaults.
	if t.Role != "client" {
		return testkit.HandleDefaultRole(t)
	}

	// This is a client role
	fastRetrieval := t.BooleanParam("fast_retrieval")
	t.RecordMessage("running client, with fast retrieval set to: %v", fastRetrieval)

	cl, err := testkit.PrepareClient(t)
	if err != nil {
		return err
	}

	ctx := context.Background()
	client := cl.FullApi

	// select a random miner
	minerAddr := cl.MinerAddrs[rand.Intn(len(cl.MinerAddrs))]
	if err := client.NetConnect(ctx, minerAddr.MinerNetAddrs); err != nil {
		return err
	}
	t.D().Counter(fmt.Sprintf("send-data-to,miner=%s", minerAddr.MinerActorAddr)).Inc(1)

	t.RecordMessage("selected %s as the miner", minerAddr.MinerActorAddr)

	if fastRetrieval {
		err = initPaymentChannel(t, ctx, cl, minerAddr)
		if err != nil {
			return err
		}
	}

	// give some time to the miner, otherwise, we get errors like:
	// deal errored deal failed: (State=26) error calling node: publishing deal: GasEstimateMessageGas
	// error: estimating gas used: message execution failed: exit 19, reason: failed to lock balance: failed to lock client funds: not enough balance to lock for addr t0102: escrow balance 0 < locked 0 + required 640297000 (RetCode=19)
	time.Sleep(50 * time.Second)

	// TODO: Support several byte sizes. Make deals and then retrieve each file size.
	fileSize := 100000
	if t.IsParamSet("file_size") {
		fileSize = t.IntParam("file_size")
	}
	t.RecordMessage("Generating random file of size: %d", fileSize)

	// generate  bytes of random data
	data := make([]byte, fileSize)
	rand.New(rand.NewSource(time.Now().UnixNano())).Read(data)

	file, err := ioutil.TempFile("/tmp", "data")
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())

	_, err = file.Write(data)
	if err != nil {
		return err
	}

	fcid, err := client.ClientImport(ctx, api.FileRef{Path: file.Name(), IsCAR: false})
	if err != nil {
		return err
	}
	t.RecordMessage("file cid: %s", fcid)

	// start deal
	t1 := time.Now()
	deal := testkit.StartDeal(ctx, minerAddr.MinerActorAddr, client, fcid.Root, fastRetrieval)
	t.RecordMessage("started deal: %s", deal)

	// TODO: this sleep is only necessary because deals don't immediately get logged in the dealstore, we should fix this
	time.Sleep(2 * time.Second)

	t.RecordMessage("waiting for deal to be sealed")
	testkit.WaitDealSealed(t, ctx, client, deal)
	t.D().ResettingHistogram("deal.sealed").Update(int64(time.Since(t1)))

	// wait for all client deals to be sealed before trying to retrieve
	t.SyncClient.MustSignalAndWait(ctx, sync.State("done-sealing"), t.IntParam("clients"))

	carExport := true
	runCount := 0

	if t.IsParamSet("run_count") {
		runCount = t.IntParam("run_count")
	}

	// Start retrievals by clients
	for i := 0; i < runCount; i++ {
		t.RecordMessage("Starting retrieval #%d for %s", i, fcid.Root)
		t1 := time.Now()
		_ = testkit.RetrieveData(t, ctx, client, fcid.Root, nil, carExport, data)
		t.R().RecordPoint(fmt.Sprintf("deal.retrieved/fast=%v (ms)", fastRetrieval), float64(time.Since(t1).Milliseconds()))

		time.Sleep(10 * time.Second) // wait for metrics to be emitted

		// TODO broadcast published content CIDs to other clients
		// TODO select a random piece of content published by some other client and retrieve it

		t.RecordMessage("Retrieval #%d for %s done!", i, fcid.Root)

		// Collect Bandwidth Stats
		collectBandwidthStats(ctx, t, cl)

		// Reset Stats for the next retrieval
		testkit.ResetStats(client)

		// Signal end of retrieval
		t.SyncClient.MustSignalAndWait(ctx, sync.State("retrieval_done"+strconv.Itoa(i)), t.IntParam("clients"))
	}

	// Stop mining
	t.SyncClient.MustSignalEntry(ctx, testkit.StateStopMining)

	time.Sleep(10 * time.Second) // wait for metrics to be emitted

	// Signal finish by all nodes
	t.SyncClient.MustSignalAndWait(ctx, testkit.StateDone, t.TestInstanceCount)
	return nil
}

func collectBandwidthStats(ctx context.Context, t *testkit.TestEnvironment, c *testkit.LotusClient) {
	stats, _ := c.FullApi.NetBandwidthStatsByProtocol(ctx)
	for k, v := range stats {
		t.R().RecordPoint(string(k)+"_totalIn", float64(v.TotalIn))
		t.R().RecordPoint(string(k)+"_totalOut", float64(v.TotalOut))
	}
}
