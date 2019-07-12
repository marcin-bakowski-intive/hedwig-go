/*
 * Copyright 2017, Automatic Inc.
 * All rights reserved.
 *
 * Author: Michael Ngo
 */

package hedwig

import (
	"context"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go/service/sqs"
)

// MessageRouteKey is a key identifying a message route
type MessageRouteKey struct {
	// Message type
	MessageType string
	// Message major version
	MessageMajorVersion int
}

// LambdaRequest contains request objects for a lambda
type LambdaRequest struct {
	// Context for request
	Context context.Context
	// SNS record for this request
	EventRecord *events.SNSEventRecord
}

// SQSRequest contains request objects for a SQS handler
type SQSRequest struct {
	// Context for request
	Context context.Context
	// SQS message for this request
	QueueMessage *sqs.Message
}

// GetLoggerFunc returns the logger object
type GetLoggerFunc func(ctx context.Context) Logger

// MessageDefaultHeadersHook is called to return default headers per message
type MessageDefaultHeadersHook func(ctx context.Context, message *Message) map[string]string

// PreProcessHookLambda is called on a sns event before any processing happens for a lambda.
// This hook may be used to perform initializations such as set up a global request id based on message headers.
type PreProcessHookLambda func(r *LambdaRequest) error

// PreProcessHookSQS is called on a message before any processing happens for a SQS queue.
// This hook may be used to perform initializations such as set up a global request id based on message headers.
type PreProcessHookSQS func(r *SQSRequest) error

// PreSerializeHook is called before a message is serialized to JSON.
// This hook may be used to modify the format over the wire.
type PreSerializeHook func(ctx *context.Context, messageData *string) error

// PreDeserializeHook is called after a message has been deserialized from JSON, but
// before a Message is created and validated. This hook may be used to modify the format over the wire.
type PreDeserializeHook func(ctx *context.Context, messageData *string) error

// Settings for Hedwig
type Settings struct {
	// AWS Region
	AWSRegion string
	// AWS account id
	AWSAccountID string
	// AWS access key
	AWSAccessKey string
	// AWS secret key
	AWSSecretKey string
	// AWS session tokenthat represents temporary credentials (i.e. for Lambda app)
	AWSSessionToken string

	// AWS read timeout for publisher
	AWSReadTimeoutS time.Duration // optional; default: 2 seconds

	// AWS debug request error logs toggle
	AWSDebugRequestLogEnabled bool

	// CallbackRegistry contains callbacks and message data factories by message type and message version
	CallbackRegistry *CallbackRegistry

	// GetLogger is a function that takes the context object and returns a logger. This may be used to plug in
	// your desired logger library. Defaults to using std library.
	// Convenience structs are provided for popular libraries: LogrusGetLoggerFunc
	GetLogger GetLoggerFunc

	// Returns default headers for a message before a message is published. This will apply to ALL messages.
	// Can be used to inject custom headers (i.e. request id).
	MessageDefaultHeadersHook MessageDefaultHeadersHook

	// Maps message type and major version to topic names
	//   <message type>, <message version> => topic name
	// An entry is required for every message type that the app wants to consumer or publish. It is
	// recommended that major versions of a message be published on separate topics.
	MessageRouting map[MessageRouteKey]string

	// Hedwig pre process hook called before any processing is done on message
	PreProcessHookLambda PreProcessHookLambda // optional
	PreProcessHookSQS    PreProcessHookSQS    // optional

	// Hedwig hook called before a message is serialized to JSON
	PreSerializeHook PreSerializeHook // optional

	// Hedwig hook called before a message has been deserialized into a Message struct
	PreDeserializeHook PreDeserializeHook // optional

	// Publisher name
	Publisher string

	// Hedwig queue name. Exclude the `HEDWIG-` prefix
	QueueName string

	// ShutdownTimeout is the time the app has to shut down before being brutally killed
	ShutdownTimeout time.Duration // optional; defaults to 10s

	// Message validator using JSON schema for validation. Additional JSON schema formats may be added.
	// Please see github.com/santhosh-tekuri/jsonschema for more details.
	Validator IMessageValidator
}

func (s *Settings) initDefaults() {
	if s.AWSReadTimeoutS == 0 {
		s.AWSReadTimeoutS = 2 * time.Second
	}
	if s.ShutdownTimeout == 0 {
		s.ShutdownTimeout = 10 * time.Second
	}
	if s.GetLogger == nil {
		stdLogger := &stdLogger{}
		s.GetLogger = func(_ context.Context) Logger { return stdLogger }
	}
}
