package main

import (
	"context"
	"encoding/json"
	"fmt"
	"k8s.io/client-go/tools/leaderelection"
	"net/http"
	"os"
	"time"
	v1 "k8s.io/api/core/v1"
	"k8s-leader-election/pkg/election"

	log "github.com/sirupsen/logrus"
	flag "github.com/spf13/pflag"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
)

var (
	flags = flag.NewFlagSet(
		`elector --election=<name>`,
		flag.ExitOnError)
	name      = flags.String("election", "", "The name of the election")
	id        = flags.String("id", "", "The id of this participant")
	ttl       = flags.Duration("ttl", 10*time.Second, "The TTL for this election")
	kubeconfigFile     = flags.String("kubeconfig", "", "Specifies path to kubeconfig file. This must be specified when not running inside a Kubernetes pod.")
	masterURL           = flags.String("masterUrl", "", "Kubernetes master endpoint")
	addr      = flags.String("http", "", "If non-empty, stand up a simple webserver that reports the leader state")
	namespace = flags.String("election-namespace", v1.NamespaceDefault, "The Kubernetes namespace for this election")
	leader = &LeaderData{}
	isleader = false
)

func makeClient() (*kubernetes.Clientset, error) {
	cfg, err := clientcmd.BuildConfigFromFlags(*masterURL, *kubeconfigFile)

	if err != nil {
		log.Fatalf("error building kubeconfig: %s", err.Error())
		return nil, err
	}

	return kubernetes.NewForConfig(cfg)
}

// LeaderData represents information about the current leader
type LeaderData struct {
	Name string `json:"name"`
}

func webHandler(res http.ResponseWriter, _ *http.Request) {
	data, err := json.Marshal(leader)
	if err != nil {
		res.WriteHeader(http.StatusInternalServerError)
		res.Write([]byte(err.Error()))
		return
	}
	res.WriteHeader(http.StatusOK)
	res.Write(data)
}

func leaderTestHandler(res http.ResponseWriter, _ *http.Request) {
	if isleader  {
		res.WriteHeader(http.StatusOK)
	} else {
		res.WriteHeader(http.StatusLocked)
	}
}

func validateFlags() {
	if len(*name) == 0 {
		log.Fatal("--election cannot be empty")
	}
}

func init() {
	log.SetFormatter(&log.JSONFormatter{})

	// Output to stdout instead of the default stderr
	// Can be any io.Writer, see below for File example
	log.SetOutput(os.Stdout)

	// Only log the warning severity or above.
	log.SetLevel(log.WarnLevel)
}

func main() {
	flags.Parse(os.Args)
	validateFlags()

	kubeClient, err := makeClient()
	if err != nil {
		log.Fatalf("error connecting to the client: %v", err)
	}

	fn := func(elector *leaderelection.LeaderElector) {
		leader.Name = elector.GetLeader()
		isleader = elector.IsLeader()
		fmt.Printf("%s is the leader\n", leader.Name)
	}

	e, err := election.NewElection(*name, *id, *namespace, *ttl, fn, kubeClient)
	if err != nil {
		log.Fatalf("failed to create election: %v", err)
	}

	ctx := context.Background()
	go election.RunElection(e, ctx)

	if len(*addr) > 0 {
		http.HandleFunc("/", webHandler)
		http.HandleFunc("/leading", leaderTestHandler)
		http.ListenAndServe(*addr, nil)
	} else {
		select {}
	}
}