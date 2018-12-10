package prune

import (
	"fmt"
	"os"
	"path/filepath"
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
	"github.com/openshift/library-go/pkg/assets"
	"github.com/openshift/library-go/pkg/operator/events"
	"github.com/openshift/library-go/pkg/operator/resource/resourceapply"
	"github.com/openshift/library-go/pkg/operator/resource/resourceread"
	"github.com/openshift/library-go/pkg/operator/staticpod/controller/common"
	"github.com/openshift/library-go/pkg/operator/staticpod/controller/installer/bindata"
)

// PruneController is a controller that watches static installer pod revision statuses and spawns
// a pruner pod to delete old revision resources from disk
type PruneController struct {
	targetNamespace, podResourcePrefix          string
	failedRevisionLimit, succeededRevisionLimit int

	// command is the string to use for the pruning pod command
	command []string
	// queue only ever has one item, but it has nice error handling backoff/retry semantics
	queue workqueue.RateLimitingInterface
	// prunerPodImageFn returns the image name for the pruning pod
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

	manifestDir           = "pkg/operator/staticpod/controller/prune"
	manifestPrunerPodPath = "manifests/pruner-pod.yaml"
	prunerPodName         = "revision-pruner"
)

// NewPruneController creates a new pruning controller
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
	if !ok {
		return nil
	}

	val, err := getRevisionLimit(revisionLimits, "failed")
	if err != nil {
		return err
	}
	c.failedRevisionLimit = val

	val, err = getRevisionLimit(revisionLimits, "succeeded")
	if err != nil {
		return err
	}
	c.succeededRevisionLimit = val

	return nil
}

func getRevisionLimit(revisionLimits map[string]interface{}, key string) (int, error) {
	val, set := revisionLimits[key]
	if !set {
		return 0, nil
	}

	limit, ok := val.(int)
	if !ok {
		return 0, fmt.Errorf("error converting %s revision limit %v to int", key, val)
	}
	return limit, nil
}

func (c *PruneController) pruneRevisionHistory(operatorStatus *operatorv1.StaticPodOperatorStatus, resourceDir string) error {
	var succeededRevisionIDs, failedRevisionIDs []int

	configMaps, err := c.kubeClient.CoreV1().ConfigMaps(c.targetNamespace).List(metav1.ListOptions{})
	if err != nil {
		return err
	}
	for _, configMap := range configMaps.Items {
		if !strings.HasPrefix(configMap.Name, statusConfigMapName) {
			continue
		}

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
		if err := c.ensurePrunePod(prunerPodName, nodeStatus.NodeName, excludedIDs[len(excludedIDs)-1], excludedIDs, nodeStatus.TargetRevision); err != nil {
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

// Example: []string{"1", "2"} will convert to: '["1","2"]'
func toJSONArray(arr []string) string {
	result, err := json.Marshal(arr)
	if err != nil {
		panic(err)
	}
	return string(result)
}

func (c PruneController) mustPrunePodTemplateAsset(nodeName string, revision int32) ([]byte, error) {
	config := struct {
		TargetNamespace string
		Name            string
		NodeName        string
		Image           string
		ImagePullPolicy string
		Command         string
		Args            string
	}{
		Name:            getPrunerPodName(nodeName, revision),
		TargetNamespace: c.targetNamespace,
		NodeName:        nodeName,
		Image:           c.prunerPodImageFn(),
		ImagePullPolicy: "Always",
		Command:         toJSONArray(c.command),
	}

	args := []string{
		fmt.Sprintf("-v=%d", 4),
		fmt.Sprintf("--max-eligible-id=%d", maxEligibleRevision),
		fmt.Sprintf("--protected-ids=%s", revisionsToString(protectedRevisions)),
		fmt.Sprintf("--resource-dir=%s", defaultResourceDir),
		fmt.Sprintf("--static-pod-name=%s", c.podResourcePrefix),
	}
	config.Args = toJSONArray(args)

	return assets.MustCreateAssetFromTemplate(manifestPrunerPodPath, bindata.MustAsset(filepath.Join(manifestDir, manifestPrunerPodPath)), config).Data, nil
}

func (c *PruneController) ensurePrunePod(podName, nodeName string, maxEligibleRevision int, protectedRevisions []int, revision int32) error {
	assetFunc := func(string) ([]byte, error) {
		return c.mustPrunerPodTemplateAsset(nodeName, revision)
	}
	directResourceResults := resourceapply.ApplyDirectly(c.kubeClient, c.eventRecorder, assetFunc, manifestPrunerPodPath)
	var errs []error
	for _, currResult := range directResourceResults {
		if currResult.Error != nil {
			errs = append(errs, fmt.Errorf("%q (%T): %v", currResult.File, currResult.Type, currResult.Error))
		}
	}
	return common.NewMultiLineAggregate(errs)
}

func getPrunerPodName(nodeName string, revision int32) string {
	return fmt.Sprintf("%s-%d-%s", prunerPodName, revision, nodeName)
}

func revisionsToString(revisions []int) string {
	values := []string{}
	for _, id := range revisions {
		value := strconv.Itoa(id)
		values = append(values, value)
	}
	return strings.Join(values, ",")
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
	err := c.syncRevisionLimits()
	if err != nil {
		return err
	}

	_, operatorStatus, _, err := c.operatorConfigClient.Get()
	if err != nil {
		return err
	}

	return c.pruneRevisionHistory(operatorStatus, defaultResourceDir)
}

// eventHandler queues the operator to check spec and status
func (c *PruneController) eventHandler() cache.ResourceEventHandler {
	return cache.ResourceEventHandlerFuncs{
		AddFunc:    func(obj interface{}) { c.queue.Add(pruneControllerWorkQueueKey) },
		UpdateFunc: func(old, new interface{}) { c.queue.Add(pruneControllerWorkQueueKey) },
		DeleteFunc: func(obj interface{}) { c.queue.Add(pruneControllerWorkQueueKey) },
	}
}
