/*
 * Copyright 2017, Automatic Inc.
 * All rights reserved.
 *
 * Author: Michael Ngo
 */

package hedwig

import (
	"context"

	"github.com/pkg/errors"
)

// IPublisher handles all publish related functions
type IPublisher interface {
	Publish(ctx context.Context, message *Message) error
}

// Publisher handles hedwig publishing for Automatic
type Publisher struct {
	awsClient IAmazonWebServicesClient
	settings  *Settings
}

// Publish a message on Hedwig
func (p *Publisher) Publish(ctx context.Context, message *Message) error {
	err := message.validate()
	if err != nil {
		return err
	}

	if p.settings.MessageDefaultHeadersHook != nil {
		defaultHeaders := p.settings.MessageDefaultHeadersHook(ctx, message)
		for k := range message.Metadata.Headers {
			defaultHeaders[k] = message.Metadata.Headers[k]
		}
		message.Metadata.Headers = defaultHeaders
	}

	messageBodyStr, err := message.JSONString()
	if err != nil {
		return err
	}
	if p.settings.PreSerializeHook != nil {
		if err := p.settings.PreSerializeHook(&ctx, &messageBodyStr); err != nil {
			return errors.Wrap(err, "Failed to process pre serialize hook")
		}
	}

	topic, err := message.topic(p.settings)
	if err != nil {
		return err
	}

	return p.awsClient.PublishSNS(ctx, p.settings, topic, messageBodyStr, message.Metadata.Headers)
}

// NewPublisher creates a new Publisher
func NewPublisher(sessionCache *AWSSessionsCache, settings *Settings) IPublisher {
	settings.initDefaults()

	return &Publisher{
		awsClient: newAWSClient(sessionCache, settings),
		settings:  settings,
	}
}
