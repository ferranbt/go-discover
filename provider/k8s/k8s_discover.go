// Package k8s provides pod discovery for Kubernetes.
package k8s

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"path/filepath"
	"strconv"

	"github.com/ericchiang/k8s"
	corev1 "github.com/ericchiang/k8s/apis/core/v1"
	"github.com/ghodss/yaml"

	"github.com/hashicorp/go-multierror"
	"github.com/mitchellh/go-homedir"
)

const (
	// AnnotationKeyPort is the annotation name of the field that specifies
	// the port name or number to append to the address.
	AnnotationKeyPort = "hashicorp.com/consul-auto-join-port"
)

type Provider struct{}

func (p *Provider) Help() string {
	return `Kubernetes (K8S):

    provider:         "k8s"
    kubeconfig:       Path to the kubeconfig file.
    namespace:        Namespace to search for pods (defaults to "default").
    label_selector:   Label selector value to filter pods.
    field_selector:   Field selector value to filter pods.
    host_network:     "true" if pod host IP and ports should be used.

    The kubeconfig file value will be searched in the following locations:

     1. Use path from "kubeconfig" option if provided.
     2. Use path from KUBECONFIG environment variable.
     3. Use default path of $HOME/.kube/config

    By default, the Pod IP is used to join. The "host_network" option may
    be set to use the Host IP. No port is used by default. Pods may set
    an annotation 'hashicorp/consul-auto-join-port' to a named port or
    an integer value. If the value matches a named port, that port will
    be used to join.

    Note that if "host_network" is set to true, then only pods that have
    a HostIP available will be selected. If a port annotation exists, then
    the port must be exposed via a HostPort as well, otherwise the pod will
    be ignored.
`
}

func (p *Provider) Addrs(args map[string]string, l *log.Logger) ([]string, error) {
	if args["provider"] != "k8s" {
		return nil, fmt.Errorf("discover-k8s: invalid provider " + args["provider"])
	}

	// First try to get the configuration from the kubeconfig value
	client, clientErr := initClientFromKubeconfig(args["kubeconfig"])
	if clientErr != nil {
		// kubeconfig failed, fall back and try in-cluster config. We do
		// this as the fallback since this makes network connections and
		// is much slower to fail.
		var err error
		client, err = k8s.NewInClusterClient()
		if err != nil {
			return nil, multierror.Append(clientErr, fmt.Errorf(
				"discover-k8s: error loading in-cluster config: %s", err))
		}
	}

	namespace := args["namespace"]
	if namespace == "" {
		namespace = "default"
	}

	var pods corev1.PodList
	err := client.List(context.Background(), namespace, &pods,
		k8s.QueryParam("labelSelector", args["label_selector"]),
		k8s.QueryParam("fieldSelector", args["field_selector"]))
	if err != nil {
		return nil, fmt.Errorf("discover-k8s: error listing pods: %s", err)
	}

	return PodAddrs(&pods, args, l)
}

func initClientFromKubeconfig(path string) (*k8s.Client, error) {
	// Get the configuration. This can come from multiple sources. We first
	// try kubeconfig it is set directly, then we fall back to in-cluster
	// auth. Finally, we try the default kubeconfig path.
	if path == "" {
		// If kubeconfig is empty, let's first try the default directory.
		// This is must faster than trying in-cluster auth so we try this
		// first.
		dir, err := homedir.Dir()
		if err != nil {
			return nil, fmt.Errorf("discover-k8s: error retrieving home directory: %s", err)
		}
		path = filepath.Join(dir, ".kube", "config")
	}

	data, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("discover-k8s: error loading kubeconfig: %s", err)
	}

	var config k8s.Config
	if err := yaml.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("discover-k8s: error parsing kubeconfig: %s", err)
	}

	return k8s.NewClient(&config)
}

// PodAddrs extracts the addresses from a list of pods.
//
// This is a separate method so that we can unit test this without having
// to setup complicated K8S cluster scenarios. It shouldn't generally be
// called externally.
func PodAddrs(pods *corev1.PodList, args map[string]string, l *log.Logger) ([]string, error) {
	hostNetwork := false
	if v := args["host_network"]; v != "" {
		var err error
		hostNetwork, err = strconv.ParseBool(v)
		if err != nil {
			return nil, fmt.Errorf("discover-k8s: host_network must be boolean value: %s", err)
		}
	}

	var addrs []string
PodLoop:
	for _, pod := range pods.Items {
		if v := pod.Status.GetPhase(); v != "Running" {
			l.Printf("[DEBUG] discover-k8s: ignoring pod %q, not running: %q",
				pod.Metadata.GetName(), v)
			continue
		}

		// If there is a Ready condition available, we need that to be true.
		// If no ready condition is set, then we accept this pod regardless.
		for _, condition := range pod.Status.Conditions {
			if condition.GetType() == "Ready" && condition.GetStatus() != "True" {
				l.Printf("[DEBUG] discover-k8s: ignoring pod %q, not ready state",
					pod.Metadata.GetName())
				continue PodLoop
			}
		}

		// Get the IP address that we will join.
		addr := pod.Status.GetPodIP()
		if hostNetwork {
			addr = pod.Status.GetHostIP()
		}
		if addr == "" {
			// This can be empty according to the API docs, so we protect that.
			l.Printf("[DEBUG] discover-k8s: ignoring pod %q, requested IP is empty",
				pod.Metadata.GetName())
			continue
		}

		// We only use the port if it is specified as an annotation. The
		// annotation value can be a name or a number.
		if pod.Metadata != nil && pod.Metadata.Annotations != nil {
			if v := pod.Metadata.Annotations[AnnotationKeyPort]; v != "" {
				port, err := podPort(pod, v, hostNetwork)
				if err != nil {
					l.Printf("[DEBUG] discover-k8s: ignoring pod %q, error retrieving port: %s",
						pod.Metadata.GetName(), err)
					continue
				}

				addr = fmt.Sprintf("%s:%d", addr, port)
			}
		}

		addrs = append(addrs, addr)
	}

	return addrs, nil
}

// podPort extracts the proper port for the address from the given pod
// for a non-empty annotation.
//
// Pre-condition: annotation is non-empty
func podPort(pod *corev1.Pod, annotation string, host bool) (int32, error) {
	// First look for a matching port matching the value of the annotation.
	for _, container := range pod.Spec.Containers {
		for _, portDef := range container.Ports {
			if portDef.GetName() == annotation {
				if host {
					// It is possible for HostPort to be zero, if that is the
					// case then we ignore this port.
					if portDef.GetHostPort() == 0 {
						continue
					}

					return portDef.GetHostPort(), nil
				}

				return portDef.GetContainerPort(), nil
			}
		}
	}

	// Otherwise assume that the port is a numeric value.
	v, err := strconv.ParseInt(annotation, 0, 32)
	return int32(v), err
}
