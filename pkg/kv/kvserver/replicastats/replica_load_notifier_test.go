package replicastats

import (
	"testing"
	"time"
)

func TestInitialState(t *testing.T) {
	subscriber := make(chan bool, 10)
	NewReplicaLoadNotifier(subscriber)

	select {
	case <-subscriber:
		t.Fatal("Expected no signal on initial state")
	default:
	}
}

func TestSingleStoreOverload(t *testing.T) {
	subscriber := make(chan bool, 10)
	notifier := NewReplicaLoadNotifier(subscriber)

	notifier.NotifyOverload("store1")
	select {
	case signal := <-subscriber:
		if !signal {
			t.Fatal("Expected true signal for overload")
		}
	case <-time.After(time.Second):
		t.Fatal("Expected signal for overload")
	}
}

func TestDuplicateOverloadSignal(t *testing.T) {
	subscriber := make(chan bool, 10)
	notifier := NewReplicaLoadNotifier(subscriber)

	notifier.NotifyOverload("store1")
	select {
	case <-subscriber:
		// Consume the first signal
	default:
	}

	notifier.NotifyOverload("store1")
	select {
	case <-subscriber:
		t.Fatal("Expected no signal for duplicate overload")
	default:
	}
}

func TestMultipleStoresOverload(t *testing.T) {
	subscriber := make(chan bool, 10)
	notifier := NewReplicaLoadNotifier(subscriber)

	notifier.NotifyOverload("store1")
	select {
	case <-subscriber:
		// Consume the first signal
	default:
	}

	notifier.NotifyOverload("store2")
	select {
	case <-subscriber:
		t.Fatal("Expected no signal for additional overload")
	default:
	}
}

func TestSingleStoreUnderload(t *testing.T) {
	subscriber := make(chan bool, 10)
	notifier := NewReplicaLoadNotifier(subscriber)

	notifier.NotifyOverload("store1")
	select {
	case <-subscriber:
		// Consume the first signal
	default:
	}

	notifier.NotifyUnderload("store1")
	select {
	case <-subscriber:
		t.Fatal("Expected no signal for partial underload")
	default:
	}
}

func TestAllStoresUnderload(t *testing.T) {
	subscriber := make(chan bool, 10)
	notifier := NewReplicaLoadNotifier(subscriber)

	notifier.NotifyOverload("store1")
	notifier.NotifyOverload("store2")
	select {
	case <-subscriber:
		// Consume the first signal
	default:
	}

	notifier.NotifyUnderload("store1")
	notifier.NotifyUnderload("store2")
	select {
	case signal := <-subscriber:
		if signal {
			t.Fatal("Expected false signal for underload")
		}
	case <-time.After(time.Second):
		t.Fatal("Expected signal for underload")
	}
}

func TestDuplicateUnderloadSignal(t *testing.T) {
	subscriber := make(chan bool, 10)
	notifier := NewReplicaLoadNotifier(subscriber)

	notifier.NotifyOverload("store1")
	notifier.NotifyOverload("store2")
	select {
	case <-subscriber:
		// Consume the first signal
	default:
	}

	notifier.NotifyUnderload("store1")
	notifier.NotifyUnderload("store2")
	select {
	case <-subscriber:
		// Consume the underload signal
	default:
	}

	notifier.NotifyUnderload("store2")
	select {
	case <-subscriber:
		t.Fatal("Expected no signal for duplicate underload")
	default:
	}
}

func TestReOverloadAfterUnderload(t *testing.T) {
	subscriber := make(chan bool, 10)
	notifier := NewReplicaLoadNotifier(subscriber)

	notifier.NotifyOverload("store1")
	select {
	case <-subscriber:
		// Consume the first signal
	default:
	}

	notifier.NotifyUnderload("store1")
	select {
	case <-subscriber:
		// Consume the underload signal
	default:
	}

	notifier.NotifyOverload("store1")
	select {
	case signal := <-subscriber:
		if !signal {
			t.Fatal("Expected true signal for re-overload")
		}
	case <-time.After(time.Second):
		t.Fatal("Expected signal for re-overload")
	}
}

func TestMixedOverloadAndUnderload(t *testing.T) {
	subscriber := make(chan bool, 10)
	notifier := NewReplicaLoadNotifier(subscriber)

	notifier.NotifyOverload("store1")
	notifier.NotifyOverload("store2")
	select {
	case <-subscriber:
		// Consume the first signal
	default:
	}

	notifier.NotifyUnderload("store1")
	select {
	case <-subscriber:
		t.Fatal("Expected no signal for mixed state")
	default:
	}
}

func TestFinalUnderload(t *testing.T) {
	subscriber := make(chan bool, 10)
	notifier := NewReplicaLoadNotifier(subscriber)

	notifier.NotifyOverload("store1")
	notifier.NotifyOverload("store2")
	select {
	case <-subscriber:
		// Consume the first signal
	default:
	}

	notifier.NotifyUnderload("store1")
	notifier.NotifyUnderload("store2")
	select {
	case signal := <-subscriber:
		if signal {
			t.Fatal("Expected false signal for final underload")
		}
	case <-time.After(time.Second):
		t.Fatal("Expected signal for final underload")
	}
}
