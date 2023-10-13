package coord

import (
	"github.com/plprobelab/zikade/internal/coord/brdcst"
	"github.com/plprobelab/zikade/internal/coord/coordt"
	"github.com/plprobelab/zikade/kadt"
	"github.com/plprobelab/zikade/pb"
)

// EventStartBroadcast starts a new
type EventStartBroadcast struct {
	QueryID coordt.QueryID
	Target  kadt.Key
	Message *pb.Message
	Seed    []kadt.PeerID
	Config  brdcst.Config
	Notify  QueryMonitor[*EventBroadcastFinished]
}

func (*EventStartBroadcast) behaviourEvent() {}

// EventBroadcastFinished is emitted by the coordinator when a broadcasting
// a record to the network has finished, either through running to completion or
// by being canceled.
type EventBroadcastFinished struct {
	QueryID   coordt.QueryID
	Contacted []kadt.PeerID
	Errors    map[string]struct {
		Node kadt.PeerID
		Err  error
	}
}

func (*EventBroadcastFinished) behaviourEvent()     {}
func (*EventBroadcastFinished) terminalQueryEvent() {}
