package hedwig

import (
	"context"
)

const sqsRequeueWaitTimeoutSeconds int64 = 5

// ListenRequest represents a request to listen for messages
type RequeueRequest struct {
	NumMessages        uint32 // default 1
	VisibilityTimeoutS uint32 // defaults to queue configuration
	LoopCount          uint32 // defaults to infinite loops
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

	for i := uint32(0); request.LoopCount == 0 || i < request.LoopCount; i++ {
		messagesNum, err := p.awsClient.RequeueDLQMessages(
			ctx, p.settings, request.NumMessages, request.VisibilityTimeoutS,
		)
		if err != nil {
			return err
		}
		if messagesNum == 0 {
			return nil
		}
	}
	return nil
}

// NewPublisher creates a new Publisher
func NewDQLProcessor(sessionCache *AWSSessionsCache, settings *Settings) IDLQProcessor {
	settings.initDefaults()

	return &DLQProcessor{
		awsClient: newAWSClient(sessionCache, settings),
		settings:  settings,
	}
}
