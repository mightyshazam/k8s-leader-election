package election

import (
	"context"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
	"k8s.io/client-go/tools/record"
	"os"
	"time"

	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/leaderelection"
)

const (
	startBackoff = time.Second
	maxBackoff   = time.Minute
)

// NewElection creates an election.  'namespace'/'election' should be an existing Kubernetes Service
// 'id' is the id if this leader, should be unique.
func NewElection(electionId, id, namespace string, ttl time.Duration, callback func(elector *leaderelection.LeaderElector), c kubernetes.Interface) (*leaderelection.LeaderElector, error) {
	_, err := c.CoreV1().Endpoints(namespace).Get(electionId, metav1.GetOptions{})
	if err != nil {
		if errors.IsNotFound(err) {
			_, err = c.CoreV1().Endpoints(namespace).Create(&v1.Endpoints{
				ObjectMeta: metav1.ObjectMeta{
					Name: electionId,
				},
			})
			if err != nil && !errors.IsConflict(err) {
				return nil, err
			}
		} else {
			return nil, err
		}
	}

	broadcaster := record.NewBroadcaster()
	hostname, err := os.Hostname()
	if err != nil {
		return nil, err
	}
	recorder := broadcaster.NewRecorder(scheme.Scheme, v1.EventSource{
		Component: "leader-elector",
		Host:      hostname,
	})

	l := &resourcelock.EndpointsLock{
		EndpointsMeta: metav1.ObjectMeta{
			Name:                       electionId,
			Namespace:                  namespace,
		},
		Client:        c.CoreV1(),
		LockConfig:    resourcelock.ResourceLockConfig{
			Identity:      id,
			EventRecorder: recorder,
		},
	}

	elector := &leaderelection.LeaderElector{}
	callbacks := leaderelection.LeaderCallbacks{
		OnStartedLeading: func(ctx context.Context) {
			callback(elector)
		},
		OnStoppedLeading: func() {
			callback(elector)
		},
		OnNewLeader: func(identity string) {
			callback(elector)
		},
	}

	config := leaderelection.LeaderElectionConfig{
		Lock:            l,
		LeaseDuration:   ttl,
		RenewDeadline:   ttl / 2,
		RetryPeriod:     ttl / 4,
		Callbacks:       callbacks,
		WatchDog:        nil,
		ReleaseOnCancel: false,
		Name:            id,
	}

	elector, err = leaderelection.NewLeaderElector(config)

	return elector, err
}

// RunElection runs an election given an leader elector.  Doesn't return.
func RunElection(e *leaderelection.LeaderElector, ctx context.Context) {
	wait.Forever(func () {
		e.Run(ctx)
	}, 0)
}