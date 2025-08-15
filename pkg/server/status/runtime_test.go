// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package status

import (
	"context"
	"math"
	"reflect"
	"runtime"
	"runtime/metrics"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/kr/pretty"
	"github.com/shirou/gopsutil/v3/net"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSumAndFilterDiskCounters(t *testing.T) {
	defer leaktest.AfterTest(t)()

	counters := []DiskStats{
		{
			ReadBytes:      1,
			readCount:      1,
			iopsInProgress: 1,
			WriteBytes:     1,
			writeCount:     1,
		},
		{
			ReadBytes:      1,
			readCount:      1,
			iopsInProgress: 1,
			WriteBytes:     1,
			writeCount:     1,
		},
	}
	summed, err := sumAndFilterDiskCounters(counters)
	if err != nil {
		t.Fatalf("error: %s", err.Error())
	}
	expected := DiskStats{
		ReadBytes:      2,
		readCount:      2,
		WriteBytes:     2,
		writeCount:     2,
		iopsInProgress: 2,
	}
	if !reflect.DeepEqual(summed, expected) {
		t.Fatalf("expected %+v; got %+v", expected, summed)
	}
}

func TestSumNetCounters(t *testing.T) {
	defer leaktest.AfterTest(t)()

	counters := []net.IOCountersStat{
		{
			BytesRecv:   1,
			PacketsRecv: 1,
			BytesSent:   1,
			PacketsSent: 1,
		},
		{
			BytesRecv:   1,
			PacketsRecv: 1,
			Errin:       1,
			Dropin:      1,
			BytesSent:   1,
			PacketsSent: 1,
			Errout:      1,
			Dropout:     1,
		},
		{
			BytesRecv:   3,
			PacketsRecv: 3,
			Errin:       1,
			Dropin:      1,
			BytesSent:   3,
			PacketsSent: 3,
			Errout:      1,
			Dropout:     1,
		},
	}
	summed := sumNetworkCounters(counters)
	expected := net.IOCountersStat{
		BytesRecv:   5,
		PacketsRecv: 5,
		Errin:       2,
		Dropin:      2,
		BytesSent:   5,
		PacketsSent: 5,
		Errout:      2,
		Dropout:     2,
	}
	if !reflect.DeepEqual(summed, expected) {
		t.Fatalf("expected %+v; got %+v", expected, summed)
	}
}

func TestSubtractDiskCounters(t *testing.T) {
	defer leaktest.AfterTest(t)()

	from := DiskStats{
		ReadBytes:      3,
		readCount:      3,
		WriteBytes:     3,
		writeCount:     3,
		iopsInProgress: 3,
	}
	sub := DiskStats{
		ReadBytes:      1,
		readCount:      1,
		iopsInProgress: 1,
		WriteBytes:     1,
		writeCount:     1,
	}
	expected := DiskStats{
		ReadBytes:  2,
		readCount:  2,
		WriteBytes: 2,
		writeCount: 2,
		// Don't touch iops in progress; it is a gauge, not a counter.
		iopsInProgress: 3,
	}
	subtractDiskCounters(&from, sub)
	if !reflect.DeepEqual(from, expected) {
		t.Fatalf("expected %+v; got %+v", expected, from)
	}
}

func TestSubtractNetCounters(t *testing.T) {
	defer leaktest.AfterTest(t)()

	from := netCounters{
		IOCounters: net.IOCountersStat{
			PacketsRecv: 3,
			BytesRecv:   3,
			Errin:       2,
			Dropin:      2,
			BytesSent:   3,
			PacketsSent: 3,
			Errout:      2,
			Dropout:     2,
		},
		TCPRetransSegs: 12,
		TCPFastRetrans: 10,
	}
	sub := netCounters{
		IOCounters: net.IOCountersStat{
			PacketsRecv: 1,
			BytesRecv:   1,
			Errin:       1,
			Dropin:      1,
			BytesSent:   1,
			PacketsSent: 1,
			Errout:      1,
			Dropout:     1,
		},
		TCPRetransSegs: 9,
		TCPFastRetrans: 4,
	}
	expected := netCounters{
		IOCounters: net.IOCountersStat{
			BytesRecv:   2,
			PacketsRecv: 2,
			Dropin:      1,
			Errin:       1,
			BytesSent:   2,
			PacketsSent: 2,
			Errout:      1,
			Dropout:     1,
		},
		TCPRetransSegs: 3,
		TCPFastRetrans: 6,
	}
	subtractNetworkCounters(&from, sub)
	if !reflect.DeepEqual(from, expected) {
		t.Fatalf("expected %+v; got %+v", expected, from)
	}
}

func TestFloat64HistogramSum(t *testing.T) {
	defer leaktest.AfterTest(t)()
	type testCase struct {
		h   metrics.Float64Histogram
		sum int64
	}
	testCases := []testCase{
		{
			h: metrics.Float64Histogram{
				Counts:  []uint64{9, 7, 6, 5, 4, 2, 0, 1, 2, 5},
				Buckets: []float64{0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100},
			},
			sum: 1485,
		},
		{
			h: metrics.Float64Histogram{
				Counts:  []uint64{9, 7, 6, 5, 4, 2, 0, 1, 2, 5},
				Buckets: []float64{math.Inf(-1), 11.1, 22.2, 33.3, 44.4, 55.5, 66.6, 77.7, 88.8, 99.9, math.Inf(1)},
			},
			sum: 1670,
		},
	}
	for _, tc := range testCases {
		require.Equal(t, tc.sum, int64(float64HistogramSum(&tc.h)))
	}
}

func TestSampleEnvironment(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	clock := hlc.NewClockForTesting(nil)

	s := NewRuntimeStatSampler(ctx, clock.WallClock())

	cgoStats := GetCGoMemStats(ctx)
	s.SampleEnvironment(ctx, cgoStats)

	require.GreaterOrEqual(t, s.HostCPUCombinedPercentNorm.Value(), s.CPUCombinedPercentNorm.Value())
}

func TestNetworkStatsLinux(t *testing.T) {
	defer leaktest.AfterTest(t)()

	if runtime.GOOS != "linux" {
		skip.IgnoreLint(t, "network stats only supported on linux")
	}

	nc, err := getSummedNetStats(context.Background())
	require.NoError(t, err)
	// Verify that we don't see -1 for TCPRetransSegs, which would indicate that
	// the RetransSegs metric is not supported on this linux system.
	require.LessOrEqual(t, int64(0), nc.TCPRetransSegs)
	require.LessOrEqual(t, int64(0), nc.TCPFastRetrans)
}

// A real file grabbed from a linux system under 10% packet loss.
const procNetStat = `
TcpExt: SyncookiesSent SyncookiesRecv SyncookiesFailed EmbryonicRsts PruneCalled RcvPruned OfoPruned OutOfWindowIcmps LockDroppedIcmps ArpFilter TW TWRecycled TWKilled PAWSActive PAWSEstab DelayedACKs DelayedACKLocked DelayedACKLost ListenOverflows ListenDrops TCPHPHits TCPPureAcks TCPHPAcks TCPRenoRecovery TCPSackRecovery TCPSACKReneging TCPSACKReorder TCPRenoReorder TCPTSReorder TCPFullUndo TCPPartialUndo TCPDSACKUndo TCPLossUndo TCPLostRetransmit TCPRenoFailures TCPSackFailures TCPLossFailures TCPFastRetrans TCPSlowStartRetrans TCPTimeouts TCPLossProbes TCPLossProbeRecovery TCPRenoRecoveryFail TCPSackRecoveryFail TCPRcvCollapsed TCPBacklogCoalesce TCPDSACKOldSent TCPDSACKOfoSent TCPDSACKRecv TCPDSACKOfoRecv TCPAbortOnData TCPAbortOnClose TCPAbortOnMemory TCPAbortOnTimeout TCPAbortOnLinger TCPAbortFailed TCPMemoryPressures TCPMemoryPressuresChrono TCPSACKDiscard TCPDSACKIgnoredOld TCPDSACKIgnoredNoUndo TCPSpuriousRTOs TCPMD5NotFound TCPMD5Unexpected TCPMD5Failure TCPSackShifted TCPSackMerged TCPSackShiftFallback TCPBacklogDrop PFMemallocDrop TCPMinTTLDrop TCPDeferAcceptDrop IPReversePathFilter TCPTimeWaitOverflow TCPReqQFullDoCookies TCPReqQFullDrop TCPRetransFail TCPRcvCoalesce TCPOFOQueue TCPOFODrop TCPOFOMerge TCPChallengeACK TCPSYNChallenge TCPFastOpenActive TCPFastOpenActiveFail TCPFastOpenPassive TCPFastOpenPassiveFail TCPFastOpenListenOverflow TCPFastOpenCookieReqd TCPFastOpenBlackhole TCPSpuriousRtxHostQueues BusyPollRxPackets TCPAutoCorking TCPFromZeroWindowAdv TCPToZeroWindowAdv TCPWantZeroWindowAdv TCPSynRetrans TCPOrigDataSent TCPHystartTrainDetect TCPHystartTrainCwnd TCPHystartDelayDetect TCPHystartDelayCwnd TCPACKSkippedSynRecv TCPACKSkippedPAWS TCPACKSkippedSeq TCPACKSkippedFinWait2 TCPACKSkippedTimeWait TCPACKSkippedChallenge TCPWinProbe TCPKeepAlive TCPMTUPFail TCPMTUPSuccess TCPDelivered TCPDeliveredCE TCPAckCompressed TCPZeroWindowDrop TCPRcvQDrop TCPWqueueTooBig TCPFastOpenPassiveAltKey TcpTimeoutRehash TcpDuplicateDataRehash TCPDSACKRecvSegs TCPDSACKIgnoredDubious TCPMigrateReqSuccess TCPMigrateReqFailure TCPPLBRehash
TcpExt: 0 0 0 99 0 0 0 5 0 0 4156 0 0 0 2 392423 218 4330 0 2 8462987 6021614 28236625 0 82 0 1532 1 51 13 23 34 581 194 0 1 2 2379 15 870 4552 262 0 0 0 183582 4322 1 4786 0 48 40 0 86 0 0 0 0 0 0 3938 0 0 0 0 728 1437 3775 0 0 0 0 0 0 0 0 5 24433719 86 0 1 0 0 0 0 0 0 0 0 0 2 0 24910503 11 11 43 70 160859949 243 18791 3 1074 6 1 1169 0 0 0 0 18437 0 0 160868740 0 0 0 0 0 0 844 36 4848 0 0 0 0
IpExt: InNoRoutes InTruncatedPkts InMcastPkts OutMcastPkts InBcastPkts OutBcastPkts InOctets OutOctets InMcastOctets OutMcastOctets InBcastOctets OutBcastOctets InCsumErrors InNoECTPkts InECT1Pkts InECT0Pkts InCEPkts ReasmOverlaps
IpExt: 0 0 0 0 0 0 374286811874 177053817186 0 0 0 0 0 106137015 0 0 0 0
MPTcpExt: MPCapableSYNRX MPCapableSYNTX MPCapableSYNACKRX MPCapableACKRX MPCapableFallbackACK MPCapableFallbackSYNACK MPFallbackTokenInit MPTCPRetrans MPJoinNoTokenFound MPJoinSynRx MPJoinSynAckRx MPJoinSynAckHMacFailure MPJoinAckRx MPJoinAckHMacFailure DSSNotMatching InfiniteMapTx InfiniteMapRx DSSNoMatchTCP DataCsumErr OFOQueueTail OFOQueue OFOMerge NoDSSInWindow DuplicateData AddAddr AddAddrTx AddAddrTxDrop EchoAdd EchoAddTx EchoAddTxDrop PortAdd AddAddrDrop MPJoinPortSynRx MPJoinPortSynAckRx MPJoinPortAckRx MismatchPortSynRx MismatchPortAckRx RmAddr RmAddrDrop RmAddrTx RmAddrTxDrop RmSubflow MPPrioTx MPPrioRx MPFailTx MPFailRx MPFastcloseTx MPFastcloseRx MPRstTx MPRstRx RcvPruned SubflowStale SubflowRecover SndWndShared RcvWndShared RcvWndConflictUpdate RcvWndConflict
MPTcpExt: 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0
`

func TestNetworkStatsFixture(t *testing.T) {
	defer leaktest.AfterTest(t)()

	{
		orig := mockableMaybeReadProcStatFile
		mockableMaybeReadProcStatFile = func(ctx context.Context, protocol, path string) (map[string]int64, error) {
			require.Equal(t, "TcpExt", protocol)
			return parseProcStatFile(ctx, protocol, []byte(procNetStat))
		}
		defer func() { mockableMaybeReadProcStatFile = orig }()
	}

	ctx := context.Background()
	nc, err := getSummedNetStats(ctx)
	require.NoError(t, err)
	nc.IOCounters = net.IOCountersStat{}
	nc.TCPRetransSegs = -1 // comes from gops, only works on linux, so override
	exp := netCounters{
		TCPRetransSegs:      -1, // not tested here
		TCPFastRetrans:      2379,
		TCPTimeouts:         870,
		TCPSlowStartRetrans: 15,
		TCPLossProbes:       4552,
	}
	require.Equal(t, exp, nc, "%s", pretty.Diff(exp, nc))
}

func TestParseProcStatFile(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()

	t.Run("success", func(t *testing.T) {
		stats, err := parseProcStatFile(ctx, "TcpExt", []byte(procNetStat))
		require.NoError(t, err)
		require.NotNil(t, stats)
		t.Log(stats)
		assert.Zero(t, stats["SynCookiesSent"])
		assert.Equal(t, int64(2379), stats["TCPFastRetrans"])
		assert.Equal(t, int64(194), stats["TCPLostRetransmit"])
		assert.Zero(t, stats["InOctets"]) // since protocol != IpExt
	})

	t.Run("protocol-not-found", func(t *testing.T) {
		stats, err := parseProcStatFile(ctx, "UdpExt", []byte(procNetStat))
		require.NoError(t, err)
		require.Nil(t, stats)
	})

	t.Run("malformed-mismatch", func(t *testing.T) {
		badData := `
TcpExt: a b
TcpExt: 1
`
		_, err := parseProcStatFile(ctx, "TcpExt", []byte(badData))
		require.Error(t, err)
		require.Contains(t, err.Error(), "mismatch between headers and values")
	})

	t.Run("malformed-parse-err", func(t *testing.T) {
		badData := `
TcpExt: a
TcpExt: b
`
		_, err := parseProcStatFile(ctx, "TcpExt", []byte(badData))
		require.Error(t, err)
		require.Contains(t, err.Error(), "could not parse value")
	})
}
