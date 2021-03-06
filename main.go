// Before running this script, run `export AWS_SHARED_CREDENTIALS_FILE="<AWS_CREDENTIALS_FILE_PATH>"` where
// AWS_CREDENTIALS_FILE_PATH represents the file having AWS credentials.
package main

import (
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"fmt"
	"os"
	"time"
	"strconv"
	"encoding/json"
	"sync/atomic"
	"sync"
	"bufio"
)

const (
	region = "eu-west-2"
	queue_name = "benchmark-queue"
	messageCount = 10000
	enque_parallelism = 10
	deque_parallelism = 10

	// Time bound enque/deque
	runtimeDuration = time.Second * 60
	enqueuingWorkers = 300
)
var sqs_client *sqs.SQS

func main() {
	createSQSClient()
	queue_url := createSQSQueue()
	message := getMessage()
	mode := os.Getenv("mode")       // Use "export mode=e" or "export mode=d" or "export mode=bd" etc.
	if mode == "e" {
		fmt.Println("Starting enqueuer")
		BulkEnqueuer(messageCount, queue_url, message)
	} else if mode == "tbe" {
		fmt.Println("Starting time bound enqueuer for duration: ", runtimeDuration)
		TimeBoundEnqueuer(runtimeDuration, queue_url, message)
	} else if mode == "d" {
		fmt.Println("Starting dequeuer")
		BulkDequeuer(messageCount, queue_url)
	} else if mode == "bd" {
		fmt.Println("Starting batch dequeuer")
		BulkBatchDequeuer(messageCount, queue_url)
	} else if mode == "tbbd" {
		fmt.Println("Starting time bound batch dequeuer for duration: ", runtimeDuration)
		TimeBoundBatchDequeuer(runtimeDuration, queue_url)
	} else {
		fmt.Println("Invalid Flag, Exiting")
		return
	}
	//EnqueueDequeueInSync(100, queue_url, message)
}

func BulkEnqueuer(itemsCount uint64, queue_url string, message string) {
	var count uint64 = 1
	var wg sync.WaitGroup
	wg.Add(enque_parallelism)
	for i:=0; i<enque_parallelism; i++ {
		go func() {
			defer wg.Done()
			for ;count <= itemsCount-enque_parallelism+1; {
				enqueue(queue_url, count, message)
				atomic.AddUint64(&count, 1)
			}
		}()
	}
	wg.Wait()
}

func TimeBoundEnqueuer(totalDuration time.Duration, queue_url string, message string) {
	var count uint64
	for i:=0; i<enqueuingWorkers; i++ {
		go func() {
			for {
				enqueue(queue_url, count, message)
				atomic.AddUint64(&count, 1)
				time.Sleep(time.Millisecond*975)
			}
		}()
	}
	select {
	        case <- time.After(totalDuration):
	                fmt.Println(count) // This may not be correct count as goroutines are still running
	                os.Exit(1)
        }
}

func BulkDequeuer(itemsCount uint64, queue_url string) {
	var totalLatency, count uint64
	count = 1
	var wg sync.WaitGroup
	wg.Add(deque_parallelism)
	for i:=0; i<deque_parallelism; i++ {
		go func() {
			defer wg.Done()
			for {
				latency := dequeue(queue_url)
				if count >= itemsCount {
					fmt.Println("Average Latency for ", count, "th item is ", totalLatency / count)
					break
				}
				if latency != 0 {
					atomic.AddUint64(&count, 1)
					atomic.AddUint64(&totalLatency, latency)
				}
			}
		}()
	}
	wg.Wait()
}

func BulkBatchDequeuer(itemsCount uint64, queue_url string) {
	var totalLatency, count uint64
	count = 1
	var wg sync.WaitGroup
	wg.Add(deque_parallelism)
	var latencyList []uint64
	for i:=0; i<deque_parallelism; i++ {
		go func() {
			defer wg.Done()
			for {
				messages := batchDequeue(queue_url)
				var entries []*sqs.DeleteMessageBatchRequestEntry
				for j:=0; j<len(messages); j++ {
					// Get latency in nanoseconds between enqueue and dequeue
					message := messages[j]
					enqueue_time := *message.MessageAttributes["EnqueueTime"].StringValue
					enqueue_time_nanosec, _ := strconv.ParseUint(enqueue_time, 10, 0)
					latency := uint64(time.Now().UnixNano()) - enqueue_time_nanosec
					//fmt.Println("Latency(ns): ", latency, count)

					// Append entries for batch delete
					id := fmt.Sprintf("%d", count)
					entries = append(entries, &sqs.DeleteMessageBatchRequestEntry{
						Id: &id,
						ReceiptHandle: message.ReceiptHandle,
					})
					if count >= itemsCount {
						fmt.Println("Average Latency for ", count, "th item is ", totalLatency / count)
						return
					}
					atomic.AddUint64(&count, 1)
					atomic.AddUint64(&totalLatency, latency)
					// Add latency to list after converting from nano to millisecond
					latencyList = append(latencyList, latency/1000000)
				}
				// Batch delete
				if len(entries) > 0 {
					_, err := sqs_client.DeleteMessageBatch(&sqs.DeleteMessageBatchInput{
						QueueUrl:      &queue_url,
						Entries:       entries,
					})
					if err != nil {
						fmt.Println("Batch Delete Error: ", err)
						os.Exit(1)
					}
				}
			}
		}()
	}
	wg.Wait()

	// Create a file and write the latencies
	file, err := os.Create("/tmp/sqs_latencies.txt")
	if err != nil {
		panic(err)
	}
	defer file.Close()
        w := bufio.NewWriter(file)
	for _, latency := range latencyList {
                fmt.Fprintln(w, latency)
        }
	if err = w.Flush(); err != nil {
                panic(err)
        }
}

func TimeBoundBatchDequeuer(totalDuration time.Duration, queue_url string) {
	var totalLatency, count uint64
	var latencyList []uint64

	for i:=0; i<deque_parallelism; i++ {
		go func() {
			for {
				messages := batchDequeue(queue_url)
				var entries []*sqs.DeleteMessageBatchRequestEntry
				for j:=0; j<len(messages); j++ {
					// Get latency in nanoseconds between enqueue and dequeue
					message := messages[j]
					enqueue_time := *message.MessageAttributes["EnqueueTime"].StringValue
					enqueue_time_nanosec, _ := strconv.ParseUint(enqueue_time, 10, 0)
					latency := uint64(time.Now().UnixNano()) - enqueue_time_nanosec
					//fmt.Println("Latency(ns): ", latency, count)

					// Append entries for batch delete
					id := fmt.Sprintf("%d", count)
					entries = append(entries, &sqs.DeleteMessageBatchRequestEntry{
						Id: &id,
						ReceiptHandle: message.ReceiptHandle,
					})
					atomic.AddUint64(&count, 1)
					atomic.AddUint64(&totalLatency, latency)
					// Add latency to list after converting from nano to millisecond
					latencyList = append(latencyList, latency/1000000)
				}
				// Batch delete
				if len(entries) > 0 {
					_, err := sqs_client.DeleteMessageBatch(&sqs.DeleteMessageBatchInput{
						QueueUrl:      &queue_url,
						Entries:       entries,
					})
					if err != nil {
						fmt.Println("Batch Delete Error: ", err)
					}
				}
			}
		}()
	}
	select {
		// Duration(plus some buffer time) is over, so write latencies to a file and exit dequeuer
	        case <- time.After(totalDuration+(time.Second*30)):
	                // Create a file and write the latencies
			file, err := os.Create("/tmp/sqs_time_bound_latencies.txt")
			if err != nil {
				panic(err)
			}
			defer file.Close()
		        w := bufio.NewWriter(file)
			for _, latency := range latencyList {
		                fmt.Fprintln(w, latency)
		        }
			if err = w.Flush(); err != nil {
		                panic(err)
		        }
	                if err = w.Flush(); err != nil {
                                panic(err)
                        }

	                fmt.Println("Average Latency for ", count, "th item is ", totalLatency / count)
	                os.Exit(1)
        }
}

// Enqueue and Dequeue one by one multiple times
func EnqueueDequeueInSync(itemsCount uint64, queue_url string, message string) {
	var i, totalLatency uint64
	for i=1; i<=itemsCount; {
		enqueue(queue_url, i, message)
		latency := dequeue(queue_url)
		if latency != 0 {
			totalLatency += latency
			//fmt.Println("Average Latency for item:", i, "is ", totalLatency / i)
			i++
		}
	}
	fmt.Println("Final average Latency for item:", i, "is ", totalLatency / i)
}

func getMessage() string{
	payload := map[string]interface{}{
	        "src": "972525626731",
	        "dst": "972502224696",
	        "prefix": "972502224696",
	        "url": "",
	        "method": "POST",
	        "text": "\u05dc\u05e7\u05d5\u05d7 \u05d9\u05e7\u05e8 \u05e2\u05e7\u05d1 \u05ea\u05e7\u05dc\u05d4 STOP",
	        "log_sms": "true",
	        "message_uuid": "ffe2bb44-d34f-4359-a7d7-217bf4e9f705",
	        "message_time": "2017-07-13 13:12:47.046303",
	        "carrier_rate": "0.0065",
	        "carrier_amount": "0.013",
	        "is_gsm": false,
	        "is_unicode": true,
	        "units": "2",
	        "auth_info": map[string]interface{}{
		        "auth_id": "MANZE1ODRHYWFIZGMXNJ",
			"auth_token": "NWRjNjU3ZDJhZDM0ZjE5NWE5ZWRmYTNmOGIzNGZm",
			"api_id": "de124d64-6186-11e7-920b-0600a1193e9b",
			"api_method": "POST",
			"api_name": "/api/v1/Message/",
			"account_id": "48844",
			"subaccount_id": "0",
			"parent_auth_id": "MANZE1ODRHYWFIZGMXNJ",
	        },
	}
	payloadBytes, _ := json.Marshal(payload)
	message := string(payloadBytes)
	return message
}

func createSQSClient() {
	// Get AWS credentials from $HOME/.aws/credentials file
	// Or Use "export AWS_SHARED_CREDENTIALS_FILE="<AWS_CREDENTIALS_FILE_PATH>""
	creds := credentials.NewSharedCredentials("", "")

	// Create aws session
	sess := session.Must(session.NewSession(&aws.Config{
		Region:      aws.String(region),
		Credentials: creds,
	}))

	// Create a SQS service client.
        sqs_client = sqs.New(sess)
}

func enqueue(queue_url string, index uint64, message string) {
	str_index := fmt.Sprintf("%d", index)
	str_timestamp := fmt.Sprintf("%d", time.Now().UnixNano())
	result, err := sqs_client.SendMessage(&sqs.SendMessageInput{
	        MessageAttributes: map[string]*sqs.MessageAttributeValue{
	            "EnqueueTime": {
	                DataType:    aws.String("Number"),
	                StringValue: aws.String(str_timestamp),
	            },
	            "Index": {
	                DataType:    aws.String("Number"),
	                StringValue: aws.String(str_index),
	            },
	        },
	        MessageBody: aws.String(message),
		QueueUrl:    &queue_url,
        })
	if err != nil {
		fmt.Println("Enqueue Error:", err, result)
	}
	//fmt.Println("Successfully enqueued messageID: ", *result.MessageId)
}

func dequeue(queue_url string) (uint64){
	result, err := sqs_client.ReceiveMessage(&sqs.ReceiveMessageInput{
		AttributeNames: []*string{
		    aws.String(sqs.MessageSystemAttributeNameSentTimestamp),    // Get only sent timestamp
		},
		MessageAttributeNames: []*string{
		    aws.String(sqs.QueueAttributeNameAll),      // Get all message attributes
		},
		QueueUrl:            &queue_url,
		MaxNumberOfMessages: aws.Int64(1),      // Return only 1 message
		VisibilityTimeout:   aws.Int64(1),      // Make message invisible for 1 second
		WaitTimeSeconds:     aws.Int64(0),      // Short polling
	})
	if err != nil {
		fmt.Println("Dequeue Error: ", err)
	}
	if len(result.Messages) == 0 {
		//fmt.Println("Dequeue Received no messages")
		return 0
	}

	// Get latency in nanoseconds between enqueue and dequeue
	message := result.Messages[0]
	enqueue_time := *message.MessageAttributes["EnqueueTime"].StringValue
	enqueue_time_nanosec, _ := strconv.ParseUint(enqueue_time, 10, 0)
	latency := uint64(time.Now().UnixNano()) - enqueue_time_nanosec
	//fmt.Println("Latency(ns): ", latency)

	// Delete the message after successful dequeue
	_, err = sqs_client.DeleteMessage(&sqs.DeleteMessageInput{
		QueueUrl:      &queue_url,
		ReceiptHandle: message.ReceiptHandle,
	})
	if err != nil {
		fmt.Println("Delete Error: ", err)
	}

	return latency
}

// Returns max(=10) messages
func batchDequeue(queue_url string) []*sqs.Message {
	result, err := sqs_client.ReceiveMessage(&sqs.ReceiveMessageInput{
		AttributeNames: []*string{
		    aws.String(sqs.MessageSystemAttributeNameSentTimestamp),    // Get only sent timestamp
		},
		MessageAttributeNames: []*string{
		    aws.String(sqs.QueueAttributeNameAll),      // Get all message attributes
		},
		QueueUrl:            &queue_url,
		MaxNumberOfMessages: aws.Int64(10),     // Returns 10 message
		VisibilityTimeout:   aws.Int64(1),      // Make message invisible for 1 second
		WaitTimeSeconds:     aws.Int64(0),      // Short polling
	})
	if err != nil {
		fmt.Println("Dequeue Error: ", err)
	}
	return result.Messages
}

func createSQSQueue() string {
	// Check if already exists
	get_result, _ := sqs_client.GetQueueUrl(&sqs.GetQueueUrlInput{
	        QueueName: aws.String(queue_name),
	})
	if get_result.QueueUrl != nil {
		fmt.Println("Queue already exists:", *get_result.QueueUrl)
		return *get_result.QueueUrl
	}

	// Create new queue
	create_result, err := sqs_client.CreateQueue(&sqs.CreateQueueInput{
                QueueName: aws.String(queue_name),
	})
        if err != nil {
                fmt.Println("Create Error: ", err)
                os.Exit(1)
        }
        fmt.Println("Successfully created new queue: ", *create_result.QueueUrl)
	return *create_result.QueueUrl
}

func deleteSQSQueue(queue_url string) {
	result, err := sqs_client.DeleteQueue(&sqs.DeleteQueueInput{
                QueueUrl: aws.String(queue_url),
	})
	if err != nil {
		fmt.Println("Delete Error: ", err)
		os.Exit(1)
	}
	fmt.Println("Queue Delelted Successfully:", result)
}

func listSQSQueues() {
	result, err := sqs_client.ListQueues(nil)
	if err != nil {
		fmt.Println("List Error: ", err)
		os.Exit(1)
	}
	fmt.Println("List of SQS Queues in", region)
	for i, urls := range result.QueueUrls {
		if urls != nil {
		    fmt.Printf("%d: %s\n", i, *urls)
		}
	}
}
