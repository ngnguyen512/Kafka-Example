package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"github.com/IBM/sarama"
	"github.com/gin-gonic/gin"
)

// Comment struct
type Comment struct {
	Text string `json:"text"`
}

func main() {
	router := gin.Default()
	api := router.Group("/api/v1") // /api

	api.POST("/comments", createComment)

	router.Run(":3000")
}

func ConnectProducer(brokersUrl []string) (sarama.SyncProducer, error) {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5

	conn, err := sarama.NewSyncProducer(brokersUrl, config)
	if err != nil {
		return nil, err
	}

	return conn, nil
}

func PushCommentToQueue(topic string, message []byte) error {
	brokersUrl := []string{"localhost:29092"}
	producer, err := ConnectProducer(brokersUrl)
	if err != nil {
		return err
	}

	defer producer.Close()

	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(message),
	}

	partition, offset, err := producer.SendMessage(msg)
	if err != nil {
		return err
	}

	fmt.Printf("Message is stored in topic(%s)/partition(%d)/offset(%d)\n", topic, partition, offset)

	return nil
}

// createComment handler
func createComment(c *gin.Context) {
	// Instantiate new Comment struct
	var cmt Comment

	// Parse JSON body into comment struct
	if err := c.ShouldBindJSON(&cmt); err != nil {
		log.Println(err)
		c.JSON(http.StatusBadRequest, gin.H{
			"success": false,
			"message": err.Error(),
		})
		return
	}

	// Convert body into bytes and send it to Kafka
	cmtInBytes, err := json.Marshal(cmt)
	if err != nil {
		log.Println(err)
		c.JSON(http.StatusInternalServerError, gin.H{
			"success": false,
			"message": err.Error(),
		})
		return
	}

	if err := PushCommentToQueue("comments", cmtInBytes); err != nil {
		log.Println(err)
		c.JSON(http.StatusInternalServerError, gin.H{
			"success": false,
			"message": err.Error(),
		})
		return
	}

	// Return Comment in JSON format
	c.JSON(http.StatusOK, gin.H{
		"success": true,
		"message": "Comment pushed successfully",
		"comment": cmt,
	})
}
