/*
 * Copyright 2018, Automatic Inc.
 * All rights reserved.
 *
 * Author: Michael Ngo
 */

package hedwig

import (
	"context"

	"time"
)

type queueConsumer struct {
	consumer
}

// ListenForMessages starts a hedwig listener for the provided message types
func (c *queueConsumer) ListenForMessages(ctx context.Context, request *ListenRequest) error {
	if request.NumMessages == 0 {
		request.NumMessages = 1
	}

	for i := uint32(0); request.LoopCount == 0 || i < request.LoopCount; i++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			if deadline, ok := ctx.Deadline(); ok {
				// is shutting down?
				if time.Until(deadline) < c.settings.ShutdownTimeout {
					return nil
				}
			}
			if err := c.awsClient.FetchAndProcessMessages(
				ctx, c.settings, request.NumMessages, request.VisibilityTimeoutS,
			); err != nil {
				return err
			}
		}
	}
	return nil
}

// NewQueueConsumer creates a new consumer object used for a queue
func NewQueueConsumer(sessionCache *AWSSessionsCache, settings *Settings) IQueueConsumer {
	awsClient := newAWSClient(sessionCache, settings)
	return NewQueueConsumerFromAwsClient(&awsClient, settings)
}

// NewQueueConsumerFromClient creates a new consumer object used for a queue
func NewQueueConsumerFromAwsClient(awsClient *IAmazonWebServicesClient, settings *Settings) IQueueConsumer {
	settings.initDefaults()
	return &queueConsumer{
		consumer: consumer{
			awsClient: *awsClient,
			settings:  settings,
		},
	}
}
