package staleconditions

import (
	"context"
	"time"

	operatorv1 "github.com/openshift/api/operator/v1"

	"github.com/openshift/library-go/pkg/controller/factory"
	"github.com/openshift/library-go/pkg/operator/events"
	"github.com/openshift/library-go/pkg/operator/v1helpers"

	"github.com/openshift/library-go/pkg/operator/trace"
)

type RemoveStaleConditionsController struct {
	conditions     []string
	operatorClient v1helpers.OperatorClient
}

func NewRemoveStaleConditionsController(
	ctx context.Context,
	conditions []string,
	operatorClient v1helpers.OperatorClient,
	eventRecorder events.Recorder,
) (context.Context, factory.Controller) {
	ctx, span := trace.TraceProvider().Tracer("library-go/remove-stale-conditions").Start(ctx, "NewRemoveStaleConditionsController")
	defer span.End()
	c := &RemoveStaleConditionsController{
		conditions:     conditions,
		operatorClient: operatorClient,
	}
	return ctx, factory.New().ResyncEvery(time.Second).WithSync(c.sync).WithInformers(operatorClient.Informer()).ToController("RemoveStaleConditionsController", eventRecorder.WithComponentSuffix("remove-stale-conditions"))
}

func (c RemoveStaleConditionsController) sync(ctx context.Context, syncContext factory.SyncContext) error {
	ctx, span := trace.TraceProvider().Tracer("library-go/remove-stale-conditions").Start(ctx, "sync")
	defer span.End()
	removeStaleConditionsFn := func(status *operatorv1.OperatorStatus) error {
		for _, condition := range c.conditions {
			v1helpers.RemoveOperatorCondition(&status.Conditions, condition)
		}
		return nil
	}

	if _, _, err := v1helpers.UpdateStatus(c.operatorClient, removeStaleConditionsFn); err != nil {
		return err
	}

	return nil
}
