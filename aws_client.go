/*
 * Copyright 2018, Automatic Inc.
 * All rights reserved.
 *
 * Author: Michael Ngo
 */

package hedwig

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/sns"
	"github.com/aws/aws-sdk-go/service/sns/snsiface"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
)

// iAmazonWebServicesClient represents an interface to the AWS client
type iAmazonWebServicesClient interface {
	FetchAndProcessMessages(ctx context.Context, settings *Settings, numMessages uint32, visibilityTimeoutS uint32) error
	HandleLambdaEvent(ctx context.Context, settings *Settings, snsEvent events.SNSEvent) error
	PublishSNS(ctx context.Context, settings *Settings, messageTopic string, payload string, headers map[string]string) error
	RequeueDLQMessages(ctx context.Context, settings *Settings, numMessages uint32, visibilityTimeoutS uint32) (int, error)
}

func getSQSQueueName(settings *Settings) string {
	return fmt.Sprintf("HEDWIG-%s", settings.QueueName)
}

// redrivePolicy model for AWS SQS RedrivePolicy attribute.
type redrivePolicy struct {
	DeadLetterTargetArn string `json:"DeadLetterTargetArn"`
}

func (a *awsClient) getSQSQueueDlqURL(ctx context.Context, queueURL string) (*string, error) {
	attributeName := "RedrivePolicy"
	queueAttrResp, err := a.sqs.GetQueueAttributesWithContext(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl:       &queueURL,
		AttributeNames: []*string{&attributeName},
	}, request.WithLogLevel(a.requestLogLevel))
	if err != nil {
		return nil, err
	}
	policy := queueAttrResp.Attributes[attributeName]
	if policy == nil || len(*policy) == 0 {
		return nil, errors.Errorf("%s attribute is null or empty string", attributeName)
	}

	jsonData := []byte(*policy)
	dlqRedrivePolicy := redrivePolicy{}
	err = json.Unmarshal(jsonData, &dlqRedrivePolicy)
	if err != nil {
		return nil, errors.Wrap(err, "invalid RedrivePolicy, unable to unmarshal")
	}
	parts := strings.Split(dlqRedrivePolicy.DeadLetterTargetArn, ":")
	dlqQueueName := parts[len(parts)-1]

	return a.getSQSQueueURL(ctx, dlqQueueName)
}

func (a *awsClient) enqueueSQSMessage(ctx context.Context, settings *Settings,
	queueMessage *sqs.Message, queueDlqURL *string, queueURL *string) error {
	loggingFields := LoggingFields{
		"message_sqs_id": *queueMessage.MessageId,
		"queue_url":      queueURL,
		"queue_dlq_url":  queueDlqURL,
	}
	sendMessageInput := sqs.SendMessageInput{
		QueueUrl:          queueURL,
		MessageBody:       queueMessage.Body,
		MessageAttributes: queueMessage.MessageAttributes,
	}
	settings.GetLogger(ctx).Info("enqueue message from DLQ", loggingFields)

	_, err := a.sqs.SendMessageWithContext(ctx, &sendMessageInput, request.WithLogLevel(a.requestLogLevel))
	if err != nil {
		return err
	}
	_, err = a.sqs.DeleteMessageWithContext(ctx, &sqs.DeleteMessageInput{
		QueueUrl:      queueDlqURL,
		ReceiptHandle: queueMessage.ReceiptHandle,
	}, request.WithLogLevel(a.requestLogLevel))
	if err != nil {
		settings.GetLogger(ctx).Error(err, "Failed to delete message from DLQ", loggingFields)
	}
	return err
}

func getSNSTopic(settings *Settings, messageTopic string) string {
	return fmt.Sprintf(
		"arn:aws:sns:%s:%s:hedwig-%s",
		settings.AWSRegion,
		settings.AWSAccountID,
		messageTopic)
}

func getRequestLogLevel(settings *Settings) aws.LogLevelType {
	var awsRequestLogLevel aws.LogLevelType
	if settings.AWSDebugRequestLogEnabled {
		awsRequestLogLevel = aws.LogDebugWithRequestErrors
	} else {
		awsRequestLogLevel = aws.LogOff
	}
	return awsRequestLogLevel
}

// awsClient wrapper struct
type awsClient struct {
	sns             snsiface.SNSAPI
	sqs             sqsiface.SQSAPI
	requestLogLevel aws.LogLevelType
}

func (a *awsClient) processSQSMessage(ctx context.Context, settings *Settings,
	queueMessage *sqs.Message, queueURL *string, queueName string, wg *sync.WaitGroup) {
	defer wg.Done()
	loggingFields := LoggingFields{
		"message_sqs_id": *queueMessage.MessageId,
	}
	sqsRequest := &SQSRequest{
		Context:      ctx,
		QueueMessage: queueMessage,
	}
	if settings.PreProcessHookSQS != nil {
		if err := settings.PreProcessHookSQS(sqsRequest); err != nil {
			settings.GetLogger(ctx).Error(err, "Failed to execute pre process hook for message", loggingFields)
			return
		}
	}

	err := a.messageHandlerSQS(settings, sqsRequest)
	switch err {
	case nil:
		_, err := a.sqs.DeleteMessageWithContext(ctx, &sqs.DeleteMessageInput{
			QueueUrl:      queueURL,
			ReceiptHandle: queueMessage.ReceiptHandle,
		}, request.WithLogLevel(a.requestLogLevel))
		if err != nil {
			settings.GetLogger(ctx).Error(err, "Failed to delete message", loggingFields)
		}
	case ErrRetry:
		settings.GetLogger(ctx).Debug("Retrying due to exception", loggingFields)
	default:
		settings.GetLogger(ctx).Error(err, "Retrying due to unknown exception", loggingFields)
	}
}

func (a *awsClient) FetchAndProcessMessages(ctx context.Context,
	settings *Settings, numMessages uint32, visibilityTimeoutS uint32) error {

	queueName := getSQSQueueName(settings)
	queueURL, err := a.getSQSQueueURL(ctx, queueName)
	if err != nil {
		return errors.Wrap(err, "failed to get SQS Queue URL")
	}

	input := &sqs.ReceiveMessageInput{
		MaxNumberOfMessages: aws.Int64(int64(numMessages)),
		QueueUrl:            queueURL,
		WaitTimeSeconds:     aws.Int64(sqsWaitTimeoutSeconds),
	}
	if visibilityTimeoutS != 0 {
		input.VisibilityTimeout = aws.Int64(int64(visibilityTimeoutS))
	}

	wg := sync.WaitGroup{}
	out, err := a.sqs.ReceiveMessageWithContext(ctx, input, request.WithLogLevel(a.requestLogLevel))
	if err != nil {
		return errors.Wrap(err, "failed to receive SQS message")
	}
	for i := range out.Messages {
		select {
		case <-ctx.Done():
			// Do nothing
		default:
			wg.Add(1)
			go a.processSQSMessage(ctx, settings, out.Messages[i], queueURL, queueName, &wg)
		}
	}
	wg.Wait()
	// if context was canceled, signal appropriately
	return ctx.Err()
}

func (a *awsClient) RequeueDLQMessages(ctx context.Context,
	settings *Settings, numMessages uint32, visibilityTimeoutS uint32) (int, error) {

	queueName := getSQSQueueName(settings)
	queueURL, err := a.getSQSQueueURL(ctx, queueName)
	if err != nil {
		return 0, errors.Wrap(err, "failed to get SQS Queue URL")
	}
	dlqQueueUrl, err := a.getSQSQueueDlqURL(ctx, *queueURL)
	if err != nil {
		return 0, errors.Wrap(err, "failed to get SQS DLQ Queue URL")
	}
	settings.GetLogger(ctx).Info("Found queue URLs", LoggingFields{
		"queue_url":     *queueURL,
		"dlq_queue_url": *dlqQueueUrl,
	})

	input := &sqs.ReceiveMessageInput{
		MaxNumberOfMessages: aws.Int64(int64(numMessages)),
		QueueUrl:            dlqQueueUrl,
		WaitTimeSeconds:     aws.Int64(sqsRequeueWaitTimeoutSeconds),
	}
	if visibilityTimeoutS != 0 {
		input.VisibilityTimeout = aws.Int64(int64(visibilityTimeoutS))
	}

	out, err := a.sqs.ReceiveMessageWithContext(ctx, input, request.WithLogLevel(a.requestLogLevel))
	if err != nil {
		return 0, errors.Wrap(err, "failed to receive SQS messages")
	}
	settings.GetLogger(ctx).Info(fmt.Sprintf("Found %d messages to enqueue", len(out.Messages)), nil)
	for i := range out.Messages {
		err = a.enqueueSQSMessage(ctx, settings, out.Messages[i], dlqQueueUrl, queueURL)
		if err != nil {
			return 0, errors.Wrap(err, "failed to enqueue SQS message")
		}
	}
	return len(out.Messages), nil
}

func (a *awsClient) processSNSRecord(ctx context.Context, settings *Settings, request *LambdaRequest) error {
	loggingFields := LoggingFields{
		"message_sns_id": request.EventRecord.SNS.MessageID,
	}

	if settings.PreProcessHookLambda != nil {
		if err := settings.PreProcessHookLambda(request); err != nil {
			settings.GetLogger(ctx).Error(
				err, "failed to execute pre process hook for lambda event", loggingFields)
			return errors.Wrapf(err, "failed to execute pre process hook")
		}
	}

	err := a.messageHandlerLambda(settings, request)
	if err != nil {
		settings.GetLogger(ctx).Error(err, "failed to process lambda event", loggingFields)
		return err
	}
	return nil
}

func (a *awsClient) HandleLambdaEvent(ctx context.Context, settings *Settings, snsEvent events.SNSEvent) error {
	wg, childCtx := errgroup.WithContext(ctx)
	for i := range snsEvent.Records {
		req := &LambdaRequest{
			Context:     childCtx,
			EventRecord: &snsEvent.Records[i],
		}
		select {
		case <-ctx.Done():
			// Do nothing
		default:
			wg.Go(func() error {
				return a.processSNSRecord(ctx, settings, req)
			})

		}
	}

	err := wg.Wait()
	if ctx.Err() != nil {
		// if context was canceled, signal appropriately
		return ctx.Err()
	}
	return err
}

// PublishSNS handles publishing to AWS SNS
func (a *awsClient) PublishSNS(ctx context.Context, settings *Settings, messageTopic string, payload string,
	headers map[string]string) error {

	topic := getSNSTopic(settings, messageTopic)

	attributes := make(map[string]*sns.MessageAttributeValue)
	for key, value := range headers {
		attributes[key] = &sns.MessageAttributeValue{
			StringValue: aws.String(value),
			DataType:    aws.String("String"),
		}
	}

	_, err := a.sns.PublishWithContext(
		ctx,
		&sns.PublishInput{
			TopicArn:          &topic,
			Message:           &payload,
			MessageAttributes: attributes,
		},
		request.WithResponseReadTimeout(settings.AWSReadTimeoutS),
		request.WithLogLevel(a.requestLogLevel),
	)
	return errors.Wrap(err, "Failed to publish message to SNS")
}

func (a *awsClient) getSQSQueueURL(ctx context.Context, queueName string) (*string, error) {
	out, err := a.sqs.GetQueueUrlWithContext(ctx, &sqs.GetQueueUrlInput{
		QueueName: &queueName,
	}, request.WithLogLevel(a.requestLogLevel))
	if err != nil {
		return nil, err
	}
	return out.QueueUrl, nil
}

func (a *awsClient) messageHandler(ctx context.Context, settings *Settings, messageBody string, receipt string,
	additionalLoggingFields LoggingFields) error {
	loggingFields := LoggingFields{
		"message_body": messageBody,
	}
	for k, v := range additionalLoggingFields {
		loggingFields[k] = v
	}
	var jsonData []byte
	if settings.PreDeserializeHook != nil {
		if err := settings.PreDeserializeHook(&ctx, &messageBody); err != nil {
			return errors.Wrapf(err, "post deserialize hook failed")
		}
	}
	jsonData = []byte(messageBody)

	if settings.CallbackRegistry == nil {
		return errors.New("callbackRegistry is required")
	}
	message := Message{
		callbackRegistry: settings.CallbackRegistry,
	}
	err := json.Unmarshal(jsonData, &message)
	if err != nil {
		settings.GetLogger(ctx).Error(err, "invalid message, unable to unmarshal", loggingFields)
		return errors.Wrapf(err, "invalid message, unable to unmarshal")
	}

	// Set validator
	message.withValidator(settings.Validator)

	err = message.validate()
	if err != nil {
		return err
	}

	err = message.validateCallback(settings)
	if err != nil {
		return err
	}

	return message.execCallback(ctx, receipt)
}

func (a *awsClient) messageHandlerSQS(settings *Settings, request *SQSRequest) error {
	loggingFields := LoggingFields{
		"message_sqs_id": *request.QueueMessage.MessageId,
	}
	return a.messageHandler(
		request.Context, settings, *request.QueueMessage.Body, *request.QueueMessage.ReceiptHandle,
		loggingFields,
	)
}

func (a *awsClient) messageHandlerLambda(settings *Settings, request *LambdaRequest) error {
	loggingFields := LoggingFields{
		"message_sns_id": request.EventRecord.SNS.MessageID,
	}
	return a.messageHandler(request.Context, settings, request.EventRecord.SNS.Message, "", loggingFields)
}

func newAWSClient(sessionCache *AWSSessionsCache, settings *Settings) iAmazonWebServicesClient {
	awsSession := sessionCache.GetSession(settings)
	awsClient := awsClient{
		sns:             sns.New(awsSession),
		sqs:             sqs.New(awsSession),
		requestLogLevel: getRequestLogLevel(settings),
	}
	return &awsClient
}
