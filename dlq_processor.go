package hedwig

import (
	"context"
	"github.com/pkg/errors"
)

const sqsRequeueWaitTimeoutSeconds int64 = 5

// ListenRequest represents a request to listen for messages
type RequeueRequest struct {
	NumMessages        uint32 // default 1
	VisibilityTimeoutS uint32 // defaults to queue configuration
}

// IPublisher handles all publish related functions
type IDLQProcessor interface {
	Requeue(ctx context.Context, request *RequeueRequest) error
}

// Publisher handles hedwig publishing for Automatic
type DLQProcessor struct {
	awsClient iAmazonWebServicesClient
	settings  *Settings
}

// Publish a message on Hedwig
func (p *DLQProcessor) Requeue(ctx context.Context, request *RequeueRequest) error {
	if request.NumMessages == 0 {
		request.NumMessages = 1
	}
	if request.NumMessages < 1 || request.NumMessages > 10 {
		return errors.Errorf("request.NumMessages=%d is not valid. Allowed values: <1-10>", request.NumMessages)
	}

	return p.awsClient.RequeueDLQMessages(ctx, p.settings, request.NumMessages, request.VisibilityTimeoutS)
}

// NewPublisher creates a new Publisher
func NewDQLProcessor(sessionCache *AWSSessionsCache, settings *Settings) IDLQProcessor {
	settings.initDefaults()

	return &DLQProcessor{
		awsClient: newAWSClient(sessionCache, settings),
		settings:  settings,
	}
}
