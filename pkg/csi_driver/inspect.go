package driver

import (
	"context"
	"encoding/json"
	"path/filepath"
	"strconv"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	corev1 "k8s.io/api/core/v1"
	pb "k8s.io/cri-api/pkg/apis/runtime/v1"
	"k8s.io/klog/v2"
)

const (
	criSocketPath = "/run/containerd/containerd.sock"

	criPodNameLabel   = "io.kubernetes.pod.name"
	criNamespaceLabel = "io.kubernetes.pod.namespace"

	preferredNumaEnv = "WORKLOAD_NIC_PREFERRED_NUMA"
)

// getNumaNodeForMount returns the NUMA container, if any, for a container that uses the specified mount path.
// The NUMA container is taken from the cloud tpu assignment, ie the existence of a WORKLOAD_NIC_PREFERRED_NUMA
// env var in the container. -1 is returned if there is no container with a NUMA preference.
func (s *nodeServer) getNumaNodeForMount(ctx context.Context, targetPath string, pod *corev1.Pod) (int, error) {
	klog.Infof("Looking for NUMA Node for %s in %s/%s", targetPath, pod.GetNamespace(), pod.GetName())

	mountName := filepath.Base(filepath.Dir(targetPath))

	criClient, conn, err := newCriClient()
	if err != nil {
		return -1, err
	}
	defer conn.Close()

	listResp, err := criClient.ListContainers(ctx, &pb.ListContainersRequest{})
		// Filter: &pb.ContainerFilter{PodSandboxId: string(pod.GetUID())},
	if err != nil {
		return -1, err
	}
	klog.Infof("found %d containers for %s/%s", len(listResp.Containers), pod.GetNamespace(), pod.GetName())
	for _, container := range listResp.Containers {
		klog.Infof("container %s: %+v", container.Id, container.Metadata)
		if container.Labels[criPodNameLabel] != pod.GetName() || container.Labels[criNamespaceLabel] != pod.GetNamespace() {
			continue
		}
		klog.Infof("found container %s/%s for %s/%s", container.Metadata.Name, container.Id, pod.GetNamespace(), pod.GetName())
		status, err := criClient.ContainerStatus(ctx, &pb.ContainerStatusRequest{
			ContainerId: container.Id,
			Verbose:     true,
		})
		if err != nil {
			klog.Errorf("Could not inspect container %s/%s, will continue to look for others: %v", container.Metadata.Name, container.Id, err)
			continue
		}
		foundPath := false
		for _, m := range status.Status.Mounts {
			if m.HostPath == targetPath {
				foundPath = true
				break
			}
		}
		if !foundPath {
			klog.Infof("%s did not have target path", container.Metadata.Name)
			continue
		} else {
			klog.Infof("%s identified with target path %s", container.Metadata.Name, targetPath)
		}

		rawInfo, found := status.Info["info"]
		if !found {
			klog.Errorf("%s did not have any info in respose, skipping", container.Metadata.Name)
			continue
		}
		var info struct {
			Config *pb.ContainerConfig `json:"config"`
		}
		if err := json.Unmarshal([]byte(rawInfo), &info); err != nil {
			klog.Errorf("%s did not have valid json info, skipping: %v", container.Metadata.Name, err)
			continue
		}
		for _, env := range info.Config.Envs {
			if env.Key != preferredNumaEnv {
				continue
			}
			node, err := strconv.Atoi(env.Value)
			if err != nil {
				klog.Errorf("%s has %s, but not a valid integer: %s; skipping", container.Metadata.Name, preferredNumaEnv, env.Value)
				break
			}
			if node < 0 {
				klog.Errorf("%s has invalid numa node %d, skipping", container.Metadata.Name, node)
				break
			}
			s.driver.RecordEventf(pod, corev1.EventTypeNormal, "MultiNICAutoAssignment", "Using NUMA node %d for %s", node, mountName)
			klog.Infof("%s using node %d; will assign to mount %s", container.Metadata.Name, node, targetPath)
			return node, nil
		}
	}

	klog.Warningf("Found no container with preferred NUMA node for %s", targetPath)
	return -1, nil
}

func newCriClient() (pb.RuntimeServiceClient, *grpc.ClientConn, error) {
	conn, err := grpc.Dial(
		"unix://"+criSocketPath,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, nil, err
	}

	// 3. Create the Runtime Service client
	return pb.NewRuntimeServiceClient(conn), conn, nil
}
