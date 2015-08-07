package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/jeffail/gabs"
	"os"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
)

func getMaxMessages() (int, error) {
	value := os.Getenv("MAX_MESSAGES")

	if "" == value {
		return 1, nil
	} else {
		return strconv.Atoi(value)
	}
}

func pollMessage(queueUrl string) {
	sqsClient := sqs.New(&aws.Config{Region: aws.String("us-east-1")})

	req := &sqs.ReceiveMessageInput{QueueURL: aws.String(queueUrl)}

	result, err := sqsClient.ReceiveMessage(req)

	if nil != err {
		panic(err)
	} else {
		parsedResponse, err := gabs.ParseJSON([]byte(*result.Messages[0].Body))

		if nil != err {
			panic(err)
		}

		var payload string

		payload = parsedResponse.Path("Message").Data().(string)

		fmt.Println(unpretty(payload))

		deleteRequest := &sqs.DeleteMessageInput{QueueURL: aws.String(queueUrl), ReceiptHandle: aws.String(*result.Messages[0].ReceiptHandle)}

		_, err = sqsClient.DeleteMessage(deleteRequest)

		if nil != err {
			panic(err)
		}
	}
}

func lookupQueueURL(topicSuffix string) (string, error) {
	sqsClient := sqs.New(&aws.Config{Region: aws.String("us-east-1")})

	req := &sqs.ListQueuesInput{}

	result, err := sqsClient.ListQueues(req)

	if nil != err {
		return "", err
	}

	for _, q := range result.QueueURLs {
		//fmt.Println(*q)

		if strings.HasSuffix(*q, topicSuffix) {
			return *q, nil
		}
	}

	return "", nil
}

func unpretty(payload string) string {
	x := []byte(payload)

	var out bytes.Buffer

	err := json.Compact(&out, x)

	if nil != err {
		panic(err)
	}

	return string(out.Bytes())
}

func main() {

	maxMessages, err := getMaxMessages()

	if nil != err {
		panic(err)
	}

	queueName, err := lookupQueueURL(os.Args[1])

	if nil != err {
		panic(err)
	} else if "" == queueName {
		os.Exit(1) // Queue not found
	}

	if -1 == maxMessages {
		for {
			pollMessage(queueName)
		}
	} else {
		for i := 0; i < maxMessages; i++ {
			pollMessage(queueName)
		}
	}
}
