package prune

import (
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/golang/glog"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"

	operatorv1 "github.com/openshift/api/operator/v1"
	"github.com/openshift/library-go/pkg/operator/events"
	"github.com/openshift/library-go/pkg/operator/resource/resourceread"
	"github.com/openshift/library-go/pkg/operator/staticpod/controller/common"
)

type PruneController struct {
	targetNamespace, podResourcePrefix          string
	failedRevisionLimit, succeededRevisionLimit int

	// command is the string to use for the pruning pod command
	command []string
	// queue only ever has one item, but it has nice error handling backoff/retry semantics
	queue workqueue.RateLimitingInterface
	// prunerPodImageFn returns the image name for the installer pod
	prunerPodImageFn func() string

	operatorConfigClient common.OperatorClient

	kubeClient    kubernetes.Interface
	eventRecorder events.Recorder

	dynamicClient      dynamic.Interface
	operatorConfigGVR  schema.GroupVersionResource
	operatorConfigName string
}

const (
	pruneControllerWorkQueueKey = "key"
	statusConfigMapName         = "revision-status"
	defaultResourceDir          = "/etc/kubernetes/static-pod-resources"
	prunerPodName               = "revision-pruner"
)

func NewPruneController(
	targetNamespace string,
	podResourcePrefix string,
	command []string,
	kubeClient kubernetes.Interface,
	operatorConfigClient common.OperatorClient,
	eventRecorder events.Recorder,
	dynamicClient dynamic.Interface,
	operatorConfigGVR schema.GroupVersionResource,
	operatorConfigName string,
) *PruneController {
	c := &PruneController{
		targetNamespace:        targetNamespace,
		podResourcePrefix:      podResourcePrefix,
		command:                command,
		failedRevisionLimit:    0,
		succeededRevisionLimit: 0,

		operatorConfigClient: operatorConfigClient,
		kubeClient:           kubeClient,
		eventRecorder:        eventRecorder,

		dynamicClient:      dynamicClient,
		operatorConfigGVR:  operatorConfigGVR,
		operatorConfigName: operatorConfigName,

		queue:            workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "PruneController"),
		prunerPodImageFn: getPrunerPodImageFromEnv,
	}

	operatorConfigClient.Informer().AddEventHandler(c.eventHandler())

	return c
}

func (c *PruneController) syncRevisionLimits() error {
	config, err := c.dynamicClient.Resource(c.operatorConfigGVR).Get(c.operatorConfigName, metav1.GetOptions{})
	if err != nil {
		return err
	}

	revisionLimits, ok, err := unstructured.NestedMap(config.Object, "spec", "operatorRevisionLimit")
	if err != nil {
		return err
	}
	if ok {
		if val, set := revisionLimits["failed"]; set {
			c.failedRevisionLimit = val.(int)
		}
		if val, set := revisionLimits["succeeded"]; set {
			c.succeededRevisionLimit = val.(int)
		}
	}
	return nil
}

func (c *PruneController) pruneRevisionHistory(operatorStatus *operatorv1.StaticPodOperatorStatus, resourceDir string) error {
	var succeededRevisionIDs, failedRevisionIDs []int

	configMaps, err := c.kubeClient.CoreV1().ConfigMaps(c.targetNamespace).List(metav1.ListOptions{})
	if err != nil {
		return err
	}
	for _, configMap := range configMaps.Items {
		if strings.HasPrefix(configMap.Name, statusConfigMapName) {
			if revision, ok := configMap.Data["revision"]; ok {
				revisionID, err := strconv.Atoi(revision)
				if err != nil {
					return err
				}
				switch configMap.Data["phase"] {
				case string(corev1.PodSucceeded):
					succeededRevisionIDs = append(succeededRevisionIDs, revisionID)
				case string(corev1.PodFailed):
					failedRevisionIDs = append(failedRevisionIDs, revisionID)
				}
			}
		}
	}

	sort.Ints(succeededRevisionIDs)
	sort.Ints(failedRevisionIDs)

	// Get list of protected IDs
	protectedSucceededRevisionIDs := succeededRevisionIDs[revisionKeyToStart(len(succeededRevisionIDs), c.succeededRevisionLimit):]
	protectedFailedRevisionIDs := failedRevisionIDs[revisionKeyToStart(len(failedRevisionIDs), c.failedRevisionLimit):]

	excludedIDs := make([]int, 0, len(protectedSucceededRevisionIDs)+len(protectedFailedRevisionIDs))
	excludedIDs = append(excludedIDs, protectedSucceededRevisionIDs...)
	excludedIDs = append(excludedIDs, protectedFailedRevisionIDs...)
	sort.Ints(excludedIDs)

	// Run pruning pod on each node and pin it to that node
	for _, nodeStatus := range operatorStatus.NodeStatuses {
		err = c.ensurePrunePod(prunerPodName, nodeStatus.NodeName, excludedIDs[len(excludedIDs)-1], excludedIDs, nodeStatus.TargetRevision)
		if err != nil {
			return err
		}
	}
	return nil
}

func revisionKeyToStart(length, limit int) int {
	if length > limit {
		return length - limit
	}
	return 0
}

func (c *PruneController) ensurePrunePod(podName, nodeName, maxEligibleRevision int, protectedRevisions []int, revision int32) error {
	required := resourceread.ReadPodV1OrDie([]byte(prunerPod))
	required.Name = fmt.Sprintf("%s-%d-%s", podName, revision, nodeName)
	required.Namespace = c.targetNamespace
	required.Spec.NodeName = nodeName
	required.Spec.Containers[0].Image = c.prunerPodImageFn()
	required.Spec.Containers[0].Command = c.command
	required.Spec.Containers[0].Args = append(required.Spec.Containers[0].Args,
		fmt.Sprintf("-v=%d", 4),
		fmt.Sprintf("--max-eligible-id=%d", maxEligibleRevision),
		fmt.Sprintf("--protected-ids=%s", revisionsToString(protectedRevisions)),
		fmt.Sprintf("--resource-dir=%s", defaultResourceDir),
		fmt.Sprintf("--static-pod-name=%s", c.podResourcePrefix),
	)

	if _, err := c.kubeClient.CoreV1().Pods(c.targetNamespace).Create(required); err != nil && !apierrors.IsAlreadyExists(err) {
		c.eventRecorder.Eventf("PrunePodFailed", "Failed to create pod for %s: %v", resourceread.WritePodV1OrDie(required), err)
		return err
	}
	return nil
}

func revisionsToString(revisions []int) string {
	revisionString := ""
	if len(revisions) > 0 {
		for i := 0; i < len(revisions)-1; i++ {
			revisionString = fmt.Sprintf("%s,%d", revisionString, revision)
		}
		revisionString = fmt.Sprintf("%s%d", revisionString, revisions[len(revisions)-1])
	}
	return revisionString
}

func getPrunerPodImageFromEnv() string {
	return os.Getenv("OPERATOR_IMAGE")
}

func (c *PruneController) Run(workers int, stopCh <-chan struct{}) {
	defer utilruntime.HandleCrash()
	defer c.queue.ShutDown()

	glog.Infof("Starting PruneController")
	defer glog.Infof("Shutting down PruneController")

	// doesn't matter what workers say, only start one.
	go wait.Until(c.runWorker, time.Second, stopCh)

	<-stopCh
}

func (c *PruneController) runWorker() {
	for c.processNextWorkItem() {
	}
}

func (c *PruneController) processNextWorkItem() bool {
	dsKey, quit := c.queue.Get()
	if quit {
		return false
	}
	defer c.queue.Done(dsKey)

	err := c.sync()
	if err == nil {
		c.queue.Forget(dsKey)
		return true
	}

	utilruntime.HandleError(fmt.Errorf("%v failed with : %v", dsKey, err))
	c.queue.AddRateLimited(dsKey)

	return true
}

func (c *PruneController) sync() error {
	_, operatorStatus, _, err := c.operatorConfigClient.Get()
	if err != nil {
		return err
	}

	err = c.syncRevisionLimits()
	if err != nil {
		return err
	}

	err = c.pruneRevisionHistory(operatorStatus, defaultResourceDir)
	if err != nil {
		return err
	}
	return nil
}

// eventHandler queues the operator to check spec and status
func (c *PruneController) eventHandler() cache.ResourceEventHandler {
	return cache.ResourceEventHandlerFuncs{
		AddFunc:    func(obj interface{}) { c.queue.Add(pruneControllerWorkQueueKey) },
		UpdateFunc: func(old, new interface{}) { c.queue.Add(pruneControllerWorkQueueKey) },
		DeleteFunc: func(obj interface{}) { c.queue.Add(pruneControllerWorkQueueKey) },
	}
}

const prunerPod = `apiVersion: v1
kind: Pod
metadata:
  namespace: <namespace>
  name: pruner-<podName>-<nodeName>
  labels:
    app: pruner
spec:
  serviceAccountName: installer-sa
  containers:
  - name: pruner
    image: ${IMAGE}
    imagePullPolicy: Always
    securityContext:
      privileged: true
      runAsUser: 0
    terminationMessagePolicy: FallbackToLogsOnError
    volumeMounts:
    - mountPath: /etc/kubernetes/
      name: kubelet-dir
  restartPolicy: Never
  securityContext:
    runAsUser: 0
  volumes:
  - hostPath:
      path: /etc/kubernetes/
    name: kubelet-dir
`
