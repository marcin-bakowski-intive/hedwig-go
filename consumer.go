/*
 * Copyright 2018, Automatic Inc.
 * All rights reserved.
 *
 * Author: Michael Ngo
 */

package hedwig

import (
	"context"

	"github.com/aws/aws-lambda-go/events"
	"github.com/pkg/errors"
)

// ListenRequest represents a request to listen for messages
type ListenRequest struct {
	NumMessages        uint32 // default 1
	VisibilityTimeoutS uint32 // defaults to queue configuration
	LoopCount          uint32 // defaults to infinite loops
}

// IQueueConsumer represents a hedwig queue consumer
type IQueueConsumer interface {
	// ListenForMessages starts a hedwig listener for the provided message types
	//
	// This function never returns by default. Possible shutdown methods:
	// 1. Cancel the context - returns immediately.
	// 2. Set a deadline on the context of less than 10 seconds - returns after processing current messages.
	// 3. Run for limited number of loops by setting LoopCount on the request - returns after running loop a finite
	// number of times
	ListenForMessages(ctx context.Context, request *ListenRequest) error
}

// ILambdaConsumer represents a lambda event consumer
type ILambdaConsumer interface {
	// HandleLambdaInput processes hedwig messages for the provided message types for Lambda apps
	HandleLambdaEvent(ctx context.Context, snsEvent events.SNSEvent) error
}

const sqsWaitTimeoutSeconds int64 = 20

// ErrRetry should cause the task to retry, but not treat the retry as an error
var ErrRetry = errors.New("Retry error")

type consumer struct {
	awsClient IAmazonWebServicesClient
	settings  *Settings
}
