package prune

import (
	"fmt"
	"io/ioutil"
	"path"
	"strings"

	"github.com/davecgh/go-spew/spew"
	"github.com/golang/glog"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"k8s.io/client-go/kubernetes"

	"github.com/openshift/library-go/pkg/config/client"
)

type PruneOptions struct {
	KubeConfig string
	KubeClient kubernetes.Interface

	MaxEligibleRevisionID int
	ProtectedRevisionIDs  []int

	ResourceDir    string
	PodManifestDir string
	StaticPodName  string
}

func NewPruneOptions() *PruneOptions {
	return &PruneOptions{}
}

func NewPrune() *cobra.Command {
	o := NewPruneOptions()

	cmd := &cobra.Command{
		Use:   "prune",
		Short: "Prune static pod installer revisions",
		Run: func(cmd *cobra.Command, args []string) {
			glog.V(1).Info(cmd.Flags())
			glog.V(1).Info(spew.Sdump(o))

			if err := o.Complete(); err != nil {
				glog.Fatal(err)
			}
			if err := o.Validate(); err != nil {
				glog.Fatal(err)
			}
			if err := o.Run(); err != nil {
				glog.Fatal(err)
			}
		},
	}

	o.AddFlags(cmd.Flags())

	return cmd
}

func (o *PruneOptions) AddFlags(fs *pflag.FlagSet) {
	fs.StringVar(&o.KubeConfig, "kubeconfig", o.KubeConfig, "kubeconfig file or empty")
	fs.IntVar(&o.MaxEligibleRevisionID, "max-eligible-id", o.MaxEligibleRevisionID, "highest revision ID to be eligible for pruning")
	fs.IntSliceVar(&o.ProtectedRevisionIDs, "protected-ids", o.ProtectedRevisionIDs, "list of revision IDs to reserve from being pruned")
	fs.StringVar(&o.ResourceDir, "resource-dir", o.ResourceDir, "directory for all files supporting the static pod manifest")
	fs.StringVar(&o.PodManifestDir, "pod-manifest-dir", o.PodManifestDir, "directory for the static pod manifest")
	fs.StringVar(&o.StaticPodName, "static-pod-name", o.StaticPodName, "name of the static pod")
}

func (o *PruneOptions) Complete() error {
	clientConfig, err := client.GetKubeConfigOrInClusterConfig(o.KubeConfig, nil)
	if err != nil {
		return err
	}
	o.KubeClient, err = kubernetes.NewForConfig(clientConfig)
	if err != nil {
		return err
	}
	return nil
}

func (o *PruneOptions) Validate() error {
	if len(o.ResourceDir) == 0 {
		return fmt.Errorf("--resource-dir is required")
	}
	if len(o.PodManifestDir) == 0 {
		return fmt.Errorf("--pod-manifest-dir is required")
	}
	if len(o.StaticPodName) == 0 {
		return fmt.Errorf("--static-pod-name is required")
	}

	if o.KubeClient == nil {
		return fmt.Errorf("missing client")
	}

	return nil
}

func (o *PruneOptions) Run() error {
	if err := o.pruneRevisions(o.ResourceDir); err != nil {
		return fmt.Errorf("error pruning installerpod resource directory revisions: %v", err)
	}
	if err := o.pruneRevisions(o.PodManifestDir); err != nil {
		return fmt.Errorf("error pruning installerpod manifest revisions: %v", err)
	}
	return nil
}

func (o *PruneOptions) pruneRevisions(directory string) error {
	protectedIDs := make(map[string]struct{}, len(o.ProtectedRevisionIDs))
	for _, id := range o.ProtectedRevisionIDs {
		protectedIDs[o.nameForRevision(id)] = struct{}{}
	}

	files, err := ioutil.ReadDir(directory)
	if err != nil {
		return err
	}

	for _, file := range files {
		// If the file is a directory AND the revision is not protected
		// AND it is a revision of the pod we want AND it is less than or equal to the max eligible revision
		if _, protected := protectedIDs[file.Name()]; !protected &&
			file.IsDir() &&
			strings.HasPrefix(file.Name(), o.StaticPodName) &&
			strings.Compare(file.Name(), o.nameForRevision(o.MaxEligibleRevisionID)) <= 0 {

			err := os.RemoveAll(path.Join(directory, file.Name()))
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (o *PruneOptions) nameForRevision(id int) string {
	return fmt.Sprintf("%s-%d", o.StaticPodName, id)
}
