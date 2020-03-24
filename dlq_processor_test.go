package hedwig

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDLQProcessor_Requeue(t *testing.T) {
	ctx := context.Background()
	settings := &Settings{
		AWSRegion:    "us-east-1",
		AWSAccountID: "1234567890",
		QueueName:    "dev-myapp",
	}
	awsClient := &FakeAWSClient{}
	numMessages := uint32(10)
	visibilityTimeoutS := uint32(10)
	awsClient.On("RequeueDLQMessages", ctx, settings, numMessages, visibilityTimeoutS).Return(nil)
	dlqProcessor := &DLQProcessor{
		awsClient: awsClient,
		settings:  settings,
	}
	requeueRequest := RequeueRequest{
		NumMessages:        numMessages,
		VisibilityTimeoutS: visibilityTimeoutS,
		LoopCount:          1,
	}
	err := dlqProcessor.Requeue(ctx, &requeueRequest)
	assert.NoError(t, err)
	awsClient.AssertExpectations(t)
	assert.Equal(t, len(awsClient.Calls), 1)
}

func TestNewDQLProcessor(t *testing.T) {
	settings := &Settings{
		AWSRegion:    "us-east-1",
		AWSAccountID: "1234567890",
		QueueName:    "dev-myapp",
	}

	sessionCache := &AWSSessionsCache{}

	iprocessor := NewDQLProcessor(sessionCache, settings)
	assert.NotNil(t, iprocessor)
}
