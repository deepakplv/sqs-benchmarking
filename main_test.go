package main

import (
	"testing"
)

func BenchmarkBulkEnqueuer(b *testing.B) {
	createSQSClient()
	queue_url := createSQSQueue()
	message := getMessage()

	b.ResetTimer()
        for n := 0; n < b.N; n++ {
                BulkEnqueuer(100, queue_url, message)
        }
}

func BenchmarkBulkDequeuer(b *testing.B) {
	createSQSClient()
	queue_url := createSQSQueue()

	b.ResetTimer()
        for n := 0; n < b.N; n++ {
                BulkDequeuer(100, queue_url)
        }
}

func BenchmarkBulkBatchDequeuer(b *testing.B) {
	createSQSClient()
	queue_url := createSQSQueue()

	b.ResetTimer()
        for n := 0; n < b.N; n++ {
                BulkBatchDequeuer(100, queue_url)
        }
}