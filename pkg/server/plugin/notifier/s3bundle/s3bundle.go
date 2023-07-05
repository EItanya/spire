package s3bundle

import (
	"bytes"
	"context"
	"encoding/pem"
	"sync"

	"cloud.google.com/go/storage"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/hcl"
	"github.com/spiffe/spire-plugin-sdk/pluginsdk"
	identityproviderv1 "github.com/spiffe/spire-plugin-sdk/proto/spire/hostservice/server/identityprovider/v1"
	notifierv1 "github.com/spiffe/spire-plugin-sdk/proto/spire/plugin/server/notifier/v1"
	plugintypes "github.com/spiffe/spire-plugin-sdk/proto/spire/plugin/types"
	configv1 "github.com/spiffe/spire-plugin-sdk/proto/spire/service/common/config/v1"
	"github.com/spiffe/spire/pkg/common/catalog"
	"google.golang.org/api/option"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func BuiltIn() catalog.BuiltIn {
	return builtIn(New())
}

func builtIn(p *Plugin) catalog.BuiltIn {
	return catalog.MakeBuiltIn("s3_bundle",
		notifierv1.NotifierPluginServer(p),
		configv1.ConfigServiceServer(p),
	)
}

type bucketClient interface {
	PutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error)
	// GetObjectGeneration(ctx context.Context, bucket, object string) (int64, error)
	// Close() s3.DeleteObjectsInput
}

type pluginConfig struct {
	Bucket     string `hcl:"bucket"`
	ObjectPath string `hcl:"object_path"`
	// ServiceAccountFile string `hcl:"service_account_file"`
}

type Plugin struct {
	notifierv1.UnsafeNotifierServer
	configv1.UnsafeConfigServer

	mu               sync.RWMutex
	log              hclog.Logger
	config           *pluginConfig
	identityProvider identityproviderv1.IdentityProviderServiceClient

	// hooks struct {
	// 	newBucketClient func(ctx context.Context, configPath string) (bucketClient, error)
	// }
}

func New() *Plugin {
	p := &Plugin{}
	// p.hooks.newBucketClient = news3BucketClient
	return p
}

func (p *Plugin) SetLogger(log hclog.Logger) {
	p.log = log
}

func (p *Plugin) BrokerHostServices(broker pluginsdk.ServiceBroker) error {
	if !broker.BrokerClient(&p.identityProvider) {
		return status.Errorf(codes.FailedPrecondition, "IdentityProvider host service is required")
	}
	return nil
}

func (p *Plugin) Notify(ctx context.Context, req *notifierv1.NotifyRequest) (*notifierv1.NotifyResponse, error) {
	config, err := p.getConfig()
	if err != nil {
		return nil, err
	}

	if _, ok := req.Event.(*notifierv1.NotifyRequest_BundleUpdated); ok {
		// ignore the bundle presented in the request. see updateBundleObject for details on why.
		if err := p.updateBundleObject(ctx, config); err != nil {
			return nil, err
		}
	}
	return &notifierv1.NotifyResponse{}, nil
}

func (p *Plugin) NotifyAndAdvise(ctx context.Context, req *notifierv1.NotifyAndAdviseRequest) (*notifierv1.NotifyAndAdviseResponse, error) {
	config, err := p.getConfig()
	if err != nil {
		return nil, err
	}

	if _, ok := req.Event.(*notifierv1.NotifyAndAdviseRequest_BundleLoaded); ok {
		// ignore the bundle presented in the request. see updateBundleObject for details on why.
		if err := p.updateBundleObject(ctx, config); err != nil {
			return nil, err
		}
	}
	return &notifierv1.NotifyAndAdviseResponse{}, nil
}
func (p *Plugin) Configure(ctx context.Context, req *configv1.ConfigureRequest) (resp *configv1.ConfigureResponse, err error) {
	config := new(pluginConfig)
	if err := hcl.Decode(&config, req.HclConfiguration); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "unable to decode configuration: %v", err)
	}

	if config.Bucket == "" {
		return nil, status.Error(codes.InvalidArgument, "bucket must be set")
	}
	if config.ObjectPath == "" {
		return nil, status.Error(codes.InvalidArgument, "object_path must be set")
	}

	p.setConfig(config)
	return &configv1.ConfigureResponse{}, nil
}

func (p *Plugin) getConfig() (*pluginConfig, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.config == nil {
		return nil, status.Error(codes.FailedPrecondition, "not configured")
	}
	return p.config, nil
}

func (p *Plugin) setConfig(config *pluginConfig) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.config = config
}

type s3BucketClient struct {
	client *s3.Client
}

func news3BucketClient(ctx context.Context, serviceAccountFile string) (bucketClient, error) {
	var opts []option.ClientOption
	if serviceAccountFile != "" {
		opts = append(opts, option.WithCredentialsFile(serviceAccountFile))
	}
	client, err := storage.NewClient(ctx, opts...)
	if err != nil {
		return nil, err
	}

	return &s3BucketClient{
		client: client,
	}, nil
}

func (c *s3BucketClient) GetObjectGeneration(ctx context.Context, bucket, object string) (int64, error) {
	atter, err := c.client.GetObjectAttributes()

	// attrs, err := c.client.Bucket(bucket).Object(object).Attrs(ctx)
	// if err != nil {
	// 	if errors.Is(err, storage.ErrObjectNotExist) {
	// 		return 0, nil
	// 	}
	// 	return 0, err
	// }
	return attrs.Generation, nil
}

func (c *s3BucketClient) PutObject(ctx context.Context, bucket, object string, data []byte, generation int64) error {
	// If for whatever reason we don't make it to w.Close(), canceling the
	// context will cleanly release resources held by the writer.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	conds := storage.Conditions{
		GenerationMatch: generation,
		DoesNotExist:    generation == 0,
	}
	w := c.client.Bucket(bucket).Object(object).If(conds).NewWriter(ctx)
	w.ContentType = "application/x-pem-file"
	if _, err := w.Write(data); err != nil {
		return err
	}
	return w.Close()
}

// bundleData formats the bundle data for storage in GCS
func bundleData(bundle *plugintypes.Bundle) []byte {
	bundleData := new(bytes.Buffer)
	for _, x509Authority := range bundle.X509Authorities {
		// no need to check the error since we're encoding into a memory buffer
		_ = pem.Encode(bundleData, &pem.Block{
			Type:  "CERTIFICATE",
			Bytes: x509Authority.Asn1,
		})
	}
	return bundleData.Bytes()
}

// func isConditionNotMetError(err error) bool {
// 	var e *googleapi.Error
// 	ok := errors.As(err, &e)
// 	if ok && e.Code == http.StatusPreconditionFailed {
// 		for _, errorItem := range e.Errors {
// 			if errorItem.Reason == "conditionNotMet" {
// 				return true
// 			}
// 		}
// 	}
// 	return false
// }
