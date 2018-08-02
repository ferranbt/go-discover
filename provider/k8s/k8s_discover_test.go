package k8s_test

import (
	"log"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	corev1 "github.com/ericchiang/k8s/apis/core/v1"
	metav1 "github.com/ericchiang/k8s/apis/meta/v1"
	discover "github.com/hashicorp/go-discover"
	"github.com/hashicorp/go-discover/provider/k8s"
)

var _ discover.Provider = (*k8s.Provider)(nil)

// Acceptance test against a real cluster
func TestAcc(t *testing.T) {
	path := "../../test/tf/k8s/kubeconfig.yaml"
	path, err := filepath.Abs(path)
	if err != nil {
		t.Fatalf("err: %s", err)
	}

	if _, err := os.Stat(path); err != nil {
		t.Skipf("Skipping, can't find %q: %s", path, err)
		return
	}

	args := discover.Config{
		"provider":       "k8s",
		"kubeconfig":     path,
		"label_selector": "app = valid",
	}

	l := log.New(os.Stderr, "", log.LstdFlags)
	p := &k8s.Provider{}
	addrs, err := p.Addrs(args, l)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("Addrs: %v", addrs)
	if len(addrs) != 3 {
		t.Fatalf("bad: %v", addrs)
	}
}

func TestPodAddrs(t *testing.T) {
	cases := []struct {
		Name     string
		Args     map[string]string
		Pods     []*corev1.Pod
		Expected []string
	}{
		{
			"Simple pods (no ready, no annotations, etc.)",
			nil,
			[]*corev1.Pod{
				&corev1.Pod{
					Status: &corev1.PodStatus{
						Phase:  stringPtr("Running"),
						PodIP:  stringPtr("1.2.3.4"),
						HostIP: stringPtr("2.3.4.5"),
					},
				},
			},
			[]string{"1.2.3.4"},
		},

		{
			"Simple pods host network",
			map[string]string{"host_network": "true"},
			[]*corev1.Pod{
				&corev1.Pod{
					Status: &corev1.PodStatus{
						Phase:  stringPtr("Running"),
						PodIP:  stringPtr("1.2.3.4"),
						HostIP: stringPtr("2.3.4.5"),
					},
				},
			},
			[]string{"2.3.4.5"},
		},

		{
			"Only running pods",
			nil,
			[]*corev1.Pod{
				&corev1.Pod{
					Status: &corev1.PodStatus{
						Phase: stringPtr("Pending"),
						PodIP: stringPtr("2.3.4.5"),
					},
				},

				&corev1.Pod{
					Status: &corev1.PodStatus{
						Phase: stringPtr("Running"),
						PodIP: stringPtr("1.2.3.4"),
					},
				},
			},
			[]string{"1.2.3.4"},
		},

		{
			"Only pods that are ready",
			nil,
			[]*corev1.Pod{
				&corev1.Pod{
					Status: &corev1.PodStatus{
						Phase: stringPtr("Pending"),
						PodIP: stringPtr("2.3.4.5"),
					},
				},

				&corev1.Pod{
					Status: &corev1.PodStatus{
						Phase: stringPtr("Running"),
						PodIP: stringPtr("ready"),
						Conditions: []*corev1.PodCondition{
							&corev1.PodCondition{
								Type:   stringPtr("Ready"),
								Status: stringPtr("True"),
							},
						},
					},
				},

				// Not true
				&corev1.Pod{
					Status: &corev1.PodStatus{
						Phase: stringPtr("Running"),
						PodIP: stringPtr("not-ready"),
						Conditions: []*corev1.PodCondition{
							&corev1.PodCondition{
								Type:   stringPtr("Ready"),
								Status: stringPtr("Unknown"),
							},
						},
					},
				},

				// Not ready type, ignored
				&corev1.Pod{
					Status: &corev1.PodStatus{
						Phase: stringPtr("Running"),
						PodIP: stringPtr("scheduled"),
						Conditions: []*corev1.PodCondition{
							&corev1.PodCondition{
								Type:   stringPtr("Scheduled"),
								Status: stringPtr("Unknown"),
							},
						},
					},
				},
			},
			[]string{"ready", "scheduled"},
		},

		{
			"Port annotation (named)",
			nil,
			[]*corev1.Pod{
				&corev1.Pod{
					Status: &corev1.PodStatus{
						Phase: stringPtr("Running"),
						PodIP: stringPtr("1.2.3.4"),
					},

					Spec: &corev1.PodSpec{
						Containers: []*corev1.Container{
							&corev1.Container{
								Ports: []*corev1.ContainerPort{
									&corev1.ContainerPort{
										Name:          stringPtr("my-port"),
										HostPort:      int32Ptr(1234),
										ContainerPort: int32Ptr(8500),
									},

									&corev1.ContainerPort{
										Name:          stringPtr("http"),
										HostPort:      int32Ptr(80),
										ContainerPort: int32Ptr(8080),
									},
								},
							},
						},
					},

					Metadata: &metav1.ObjectMeta{
						Annotations: map[string]string{
							k8s.AnnotationKeyPort: "my-port",
						},
					},
				},
			},
			[]string{"1.2.3.4:8500"},
		},

		{
			"Port annotation (named with host network)",
			map[string]string{"host_network": "true"},
			[]*corev1.Pod{
				&corev1.Pod{
					Status: &corev1.PodStatus{
						Phase:  stringPtr("Running"),
						PodIP:  stringPtr("1.2.3.4"),
						HostIP: stringPtr("2.3.4.5"),
					},

					Spec: &corev1.PodSpec{
						Containers: []*corev1.Container{
							&corev1.Container{
								Ports: []*corev1.ContainerPort{
									&corev1.ContainerPort{
										Name:          stringPtr("http"),
										HostPort:      int32Ptr(80),
										ContainerPort: int32Ptr(8080),
									},
								},
							},
						},
					},

					Metadata: &metav1.ObjectMeta{
						Annotations: map[string]string{
							k8s.AnnotationKeyPort: "http",
						},
					},
				},
			},
			[]string{"2.3.4.5:80"},
		},

		{
			"Port annotation (direct)",
			nil,
			[]*corev1.Pod{
				&corev1.Pod{
					Status: &corev1.PodStatus{
						Phase: stringPtr("Running"),
						PodIP: stringPtr("1.2.3.4"),
					},

					Spec: &corev1.PodSpec{
						Containers: []*corev1.Container{
							&corev1.Container{
								Ports: []*corev1.ContainerPort{
									&corev1.ContainerPort{
										Name:          stringPtr("http"),
										HostPort:      int32Ptr(80),
										ContainerPort: int32Ptr(8080),
									},
								},
							},
						},
					},

					Metadata: &metav1.ObjectMeta{
						Annotations: map[string]string{
							k8s.AnnotationKeyPort: "4600",
						},
					},
				},
			},
			[]string{"1.2.3.4:4600"},
		},

		{
			"Port annotation (direct with host network)",
			map[string]string{"host_network": "true"},
			[]*corev1.Pod{
				&corev1.Pod{
					Status: &corev1.PodStatus{
						Phase:  stringPtr("Running"),
						PodIP:  stringPtr("1.2.3.4"),
						HostIP: stringPtr("2.3.4.5"),
					},

					Spec: &corev1.PodSpec{
						Containers: []*corev1.Container{
							&corev1.Container{
								Ports: []*corev1.ContainerPort{
									&corev1.ContainerPort{
										Name:          stringPtr("http"),
										HostPort:      int32Ptr(80),
										ContainerPort: int32Ptr(8080),
									},
								},
							},
						},
					},

					Metadata: &metav1.ObjectMeta{
						Annotations: map[string]string{
							k8s.AnnotationKeyPort: "4600",
						},
					},
				},
			},
			[]string{"2.3.4.5:4600"},
		},
	}

	for _, tt := range cases {
		t.Run(tt.Name, func(t *testing.T) {
			l := log.New(os.Stderr, "", log.LstdFlags)
			addrs, err := k8s.PodAddrs(&corev1.PodList{Items: tt.Pods}, tt.Args, l)
			if err != nil {
				t.Fatalf("err: %s", err)
			}

			if !reflect.DeepEqual(addrs, tt.Expected) {
				t.Fatalf("bad: %#v", addrs)
			}
		})
	}
}

func stringPtr(v string) *string { return &v }
func int32Ptr(v int32) *int32    { return &v }
