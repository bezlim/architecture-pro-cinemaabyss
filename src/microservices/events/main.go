package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/logger"
	"github.com/segmentio/kafka-go"
)

// Kafka configuration
var (
	kafkaBrokers    []string
	movieTopic      = "movie-events"
	userTopic       = "user-events"
	paymentTopic    = "payment-events"
	kafkaWriter     *kafka.Writer
	kafkaReader     *kafka.Reader
)

// Event models
type MovieEvent struct {
	MovieID     int      `json:"movie_id"`
	Title       string   `json:"title"`
	Action      string   `json:"action"`
	UserID      *int     `json:"user_id,omitempty"`
	Rating      *float64 `json:"rating,omitempty"`
	Genres      []string `json:"genres,omitempty"`
	Description *string  `json:"description,omitempty"`
}

type UserEvent struct {
	UserID    int       `json:"user_id"`
	Username  *string   `json:"username,omitempty"`
	Email     *string   `json:"email,omitempty"`
	Action    string    `json:"action"`
	Timestamp time.Time `json:"timestamp"`
}

type PaymentEvent struct {
	PaymentID  int       `json:"payment_id"`
	UserID     int       `json:"user_id"`
	Amount     float64   `json:"amount"`
	Status     string    `json:"status"`
	Timestamp  time.Time `json:"timestamp"`
	MethodType *string   `json:"method_type,omitempty"`
}

type EventResponse struct {
	Status    string      `json:"status"`
	Partition int         `json:"partition"`
	Offset    int64       `json:"offset"`
	Event     interface{} `json:"event"`
}

func main() {
	// Load configuration
	kafkaBrokers = []string{getEnv("KAFKA_BROKERS", "localhost:9092")}

	// Initialize Kafka writer
	kafkaWriter = &kafka.Writer{
		Addr:         kafka.TCP(kafkaBrokers...),
		Balancer:     &kafka.LeastBytes{},
		RequiredAcks: kafka.RequireOne,
	}
	defer kafkaWriter.Close()

	// Initialize Kafka reader for consuming events (MVP - just for demonstration)
	kafkaReader = kafka.NewReader(kafka.ReaderConfig{
		Brokers:   kafkaBrokers,
		Topic:     movieTopic,
		MinBytes:  10e3,
		MaxBytes:  10e6,
		MaxWait:   10 * time.Second,
		StartOffset: kafka.LastOffset,
	})
	defer kafkaReader.Close()

	// Start consumer goroutine
	go consumeEvents()

	// Initialize Fiber app
	app := fiber.New(fiber.Config{
		AppName:      "CinemaAbyss Events Service",
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  10 * time.Second,
	})

	// Middleware
	app.Use(logger.New())

	// Health check endpoint
	app.Get("/api/events/health", healthHandler)

	// Event endpoints
	app.Post("/api/events/movie", createMovieEvent)
	app.Post("/api/events/user", createUserEvent)
	app.Post("/api/events/payment", createPaymentEvent)

	// Start server
	port := getEnv("PORT", "8082")
	log.Printf("Starting Events Service on port %s", port)
	log.Printf("Kafka Brokers: %v", kafkaBrokers)
	log.Fatal(app.Listen(":" + port))
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func healthHandler(c *fiber.Ctx) error {
	return c.JSON(fiber.Map{
		"status": true,
	})
}

func createMovieEvent(c *fiber.Ctx) error {
	var event MovieEvent
	if err := json.NewDecoder(bytes.NewReader(c.Body())).Decode(&event); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": err.Error(),
		})
	}

	// Create Kafka message
	message := kafka.Message{
		Key:   []byte(fmt.Sprintf("movie-%d", event.MovieID)),
		Value: mustMarshal(event),
		Topic: movieTopic,
	}

	// Publish to Kafka
	ctx := c.Context()
	err := kafkaWriter.WriteMessages(ctx, message)
	if err != nil {
		log.Printf("Failed to publish movie event: %v", err)
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": err.Error(),
		})
	}

	log.Printf("Published movie event: movie_id=%d, action=%s", event.MovieID, event.Action)

	// Log event processing (MVP - just logging)
	log.Printf("Processing movie event: %+v", event)

	return c.Status(fiber.StatusCreated).JSON(EventResponse{
		Status:    "success",
		Partition: 0,
		Offset:    0,
		Event:     event,
	})
}

func createUserEvent(c *fiber.Ctx) error {
	var event UserEvent
	if err := json.NewDecoder(bytes.NewReader(c.Body())).Decode(&event); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": err.Error(),
		})
	}

	// Create Kafka message
	message := kafka.Message{
		Key:   []byte(fmt.Sprintf("user-%d", event.UserID)),
		Value: mustMarshal(event),
		Topic: userTopic,
	}

	// Publish to Kafka
	ctx := c.Context()
	err := kafkaWriter.WriteMessages(ctx, message)
	if err != nil {
		log.Printf("Failed to publish user event: %v", err)
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": err.Error(),
		})
	}

	log.Printf("Published user event: user_id=%d, action=%s", event.UserID, event.Action)

	// Log event processing (MVP - just logging)
	log.Printf("Processing user event: %+v", event)

	return c.Status(fiber.StatusCreated).JSON(EventResponse{
		Status:    "success",
		Partition: 0,
		Offset:    0,
		Event:     event,
	})
}

func createPaymentEvent(c *fiber.Ctx) error {
	var event PaymentEvent
	if err := json.NewDecoder(bytes.NewReader(c.Body())).Decode(&event); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": err.Error(),
		})
	}

	// Create Kafka message
	message := kafka.Message{
		Key:   []byte(fmt.Sprintf("payment-%d", event.PaymentID)),
		Value: mustMarshal(event),
		Topic: paymentTopic,
	}

	// Publish to Kafka
	ctx := c.Context()
	err := kafkaWriter.WriteMessages(ctx, message)
	if err != nil {
		log.Printf("Failed to publish payment event: %v", err)
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": err.Error(),
		})
	}

	log.Printf("Published payment event: payment_id=%d, status=%s", event.PaymentID, event.Status)

	// Log event processing (MVP - just logging)
	log.Printf("Processing payment event: %+v", event)

	return c.Status(fiber.StatusCreated).JSON(EventResponse{
		Status:    "success",
		Partition: 0,
		Offset:    0,
		Event:     event,
	})
}

func consumeEvents() {
	log.Println("Starting Kafka consumer...")
	for {
		msg, err := kafkaReader.ReadMessage(context.Background())
		if err != nil {
			log.Printf("Failed to read message: %v", err)
			continue
		}

		log.Printf("Consumed event from topic=%s partition=%d offset=%d key=%s",
			msg.Topic, msg.Partition, msg.Offset, string(msg.Key))
		log.Printf("Event payload: %s", string(msg.Value))
	}
}

func mustMarshal(v interface{}) []byte {
	data, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return data
}
