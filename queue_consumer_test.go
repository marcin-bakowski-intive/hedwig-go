/*
 * Copyright 2018, Automatic Inc.
 * All rights reserved.
 *
 * Author: Michael Ngo
 */

package hedwig

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"time"
)

func TestConsumer_ListenForMessages(t *testing.T) {
	ctx := context.Background()
	settings := &Settings{
		AWSRegion:    "us-east-1",
		AWSAccountID: "1234567890",
		QueueName:    "dev-myapp",
	}
	awsClient := &FakeAWSClient{}
	numMessages := uint32(10)
	visibilityTimeoutS := uint32(10)
	awsClient.On("FetchAndProcessMessages", ctx, settings, numMessages, visibilityTimeoutS).Return(nil)
	consumer := queueConsumer{
		consumer: consumer{
			awsClient: awsClient,
			settings:  settings,
		},
	}
	listenRequest := ListenRequest{
		NumMessages:        numMessages,
		VisibilityTimeoutS: visibilityTimeoutS,
		LoopCount:          5,
	}
	err := consumer.ListenForMessages(ctx, &listenRequest)
	assert.NoError(t, err)
	awsClient.AssertExpectations(t)
	assert.Equal(t, len(awsClient.Calls), int(listenRequest.LoopCount))
}

func TestConsumer_ListenForMessagesContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	settings := &Settings{
		AWSRegion:    "us-east-1",
		AWSAccountID: "1234567890",
		QueueName:    "dev-myapp",
	}
	awsClient := &FakeAWSClient{}
	numMessages := uint32(10)
	visibilityTimeoutS := uint32(10)
	awsClient.On("FetchAndProcessMessages", ctx, settings, numMessages, visibilityTimeoutS).
		Return(nil).
		After(500 * time.Millisecond)
	consumer := queueConsumer{
		consumer: consumer{
			awsClient: awsClient,
			settings:  settings,
		},
	}
	ch := make(chan bool)
	go func() {
		err := consumer.ListenForMessages(ctx, &ListenRequest{
			NumMessages:        numMessages,
			VisibilityTimeoutS: visibilityTimeoutS,
			LoopCount:          1000,
		})
		assert.EqualError(t, err, "context canceled")
		ch <- true
		close(ch)
	}()
	time.Sleep(1 * time.Millisecond)
	cancel()
	// wait for co-routine to finish
	<-ch
	awsClient.AssertExpectations(t)
	assert.True(t, len(awsClient.Calls) < 1000)
}

func TestConsumer_ListenForMessagesContextDeadline(t *testing.T) {
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(11*time.Second))
	settings := &Settings{
		AWSRegion:    "us-east-1",
		AWSAccountID: "1234567890",
		QueueName:    "dev-myapp",
	}
	awsClient := &FakeAWSClient{}
	numMessages := uint32(10)
	visibilityTimeoutS := uint32(10)
	awsClient.On("FetchAndProcessMessages", ctx, settings, numMessages, visibilityTimeoutS).
		Return(nil).
		After(500 * time.Millisecond)
	consumer := queueConsumer{
		consumer: consumer{
			awsClient: awsClient,
			settings:  settings,
		},
	}
	ch := make(chan bool)
	go func() {
		err := consumer.ListenForMessages(ctx, &ListenRequest{
			NumMessages:        numMessages,
			VisibilityTimeoutS: visibilityTimeoutS,
			LoopCount:          1000,
		})
		assert.EqualError(t, err, "context canceled")
		ch <- true
		close(ch)
	}()
	time.Sleep(1 * time.Millisecond)
	cancel()
	// wait for co-routine to finish
	<-ch
	awsClient.AssertExpectations(t)
	assert.True(t, len(awsClient.Calls) < 1000)
}

func TestNewQueueConsumer(t *testing.T) {
	settings := &Settings{
		AWSRegion:    "us-east-1",
		AWSAccountID: "1234567890",
		QueueName:    "dev-myapp",
	}

	sessionCache := &AWSSessionsCache{}

	iconsumer := NewQueueConsumer(sessionCache, settings)
	assert.NotNil(t, iconsumer)
}

func TestNewQueueConsumerFromAwsClient(t *testing.T) {
	settings := &Settings{
		AWSRegion:    "us-east-1",
		AWSAccountID: "1234567890",
		QueueName:    "dev-myapp",
	}
	awsSession := NewAWSSessionsCache()
	awsClient := newAWSClient(awsSession, settings)

	iconsumer := NewQueueConsumerFromAwsClient(&awsClient, settings)
	assert.NotNil(t, iconsumer)
}
