package main

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"strconv"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/logger"
	"github.com/valyala/fasthttp"
)

// Config holds the proxy configuration
type Config struct {
	Port                  string
	MonolithURL           string
	MoviesServiceURL      string
	EventsServiceURL      string
	GradualMigration      bool
	MoviesMigrationPercent int
}

// Proxy holds the reverse proxy clients
type Proxy struct {
	Config        *Config
	Monolith      *fasthttp.Client
	MoviesService *fasthttp.Client
	EventsService *fasthttp.Client
}

func main() {
	// Initialize configuration
	cfg := loadConfig()

	// Initialize proxy
	proxy := &Proxy{
		Config: cfg,
		Monolith: &fasthttp.Client{
			MaxConnsPerHost: 100,
			ReadTimeout:     10 * time.Second,
			WriteTimeout:    10 * time.Second,
		},
		MoviesService: &fasthttp.Client{
			MaxConnsPerHost: 100,
			ReadTimeout:     10 * time.Second,
			WriteTimeout:    10 * time.Second,
		},
		EventsService: &fasthttp.Client{
			MaxConnsPerHost: 100,
			ReadTimeout:     10 * time.Second,
			WriteTimeout:    10 * time.Second,
		},
	}

	// Initialize Fiber app
	app := fiber.New(fiber.Config{
		AppName:      "CinemaAbyss Proxy Service",
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  10 * time.Second,
	})

	// Middleware
	app.Use(logger.New())

	// Health check endpoint
	app.Get("/health", proxy.healthHandler)

	// API routes
	app.All("/api/movies/*", proxy.moviesHandler)
	app.All("/api/movies", proxy.moviesHandler)
	app.All("/api/users/*", proxy.usersHandler)
	app.All("/api/users", proxy.usersHandler)
	app.All("/api/payments/*", proxy.paymentsHandler)
	app.All("/api/payments", proxy.paymentsHandler)
	app.All("/api/subscriptions/*", proxy.subscriptionsHandler)
	app.All("/api/subscriptions", proxy.subscriptionsHandler)
	app.All("/api/events/*", proxy.eventsHandler)
	app.All("/api/events", proxy.eventsHandler)

	// Start server
	log.Printf("Starting Proxy Service on port %s", cfg.Port)
	log.Printf("Monolith URL: %s", cfg.MonolithURL)
	log.Printf("Movies Service URL: %s", cfg.MoviesServiceURL)
	log.Printf("Events Service URL: %s", cfg.EventsServiceURL)
	log.Printf("Gradual Migration: %v", cfg.GradualMigration)
	log.Printf("Movies Migration Percent: %d%%", cfg.MoviesMigrationPercent)

	log.Fatal(app.Listen(":" + cfg.Port))
}

func loadConfig() *Config {
	gradualMigration := os.Getenv("GRADUAL_MIGRATION") == "true"
	migrationPercent := 50

	if percent := os.Getenv("MOVIES_MIGRATION_PERCENT"); percent != "" {
		if p, err := strconv.Atoi(percent); err == nil {
			migrationPercent = p
		}
	}

	return &Config{
		Port:                  getEnv("PORT", "8000"),
		MonolithURL:           getEnv("MONOLITH_URL", "http://localhost:8080"),
		MoviesServiceURL:      getEnv("MOVIES_SERVICE_URL", "http://localhost:8081"),
		EventsServiceURL:      getEnv("EVENTS_SERVICE_URL", "http://localhost:8082"),
		GradualMigration:      gradualMigration,
		MoviesMigrationPercent: migrationPercent,
	}
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

// healthHandler returns the health status of the proxy
func (p *Proxy) healthHandler(c *fiber.Ctx) error {
	return c.SendString("Strangler Fig Proxy is healthy")
}

// moviesHandler handles requests to /api/movies
func (p *Proxy) moviesHandler(c *fiber.Ctx) error {
	var targetURL string

	// Strangler Fig: gradual migration for movies endpoint
	if p.Config.GradualMigration && shouldRouteToMoviesService(p.Config.MoviesMigrationPercent) {
		targetURL = p.Config.MoviesServiceURL
		log.Printf("Routing /api/movies to Movies Service (migration: %d%%)", p.Config.MoviesMigrationPercent)
	} else {
		targetURL = p.Config.MonolithURL
		log.Printf("Routing /api/movies to Monolith")
	}

	return p.proxyRequest(c, targetURL)
}

// usersHandler handles requests to /api/users
func (p *Proxy) usersHandler(c *fiber.Ctx) error {
	log.Printf("Routing /api/users to Monolith")
	return p.proxyRequest(c, p.Config.MonolithURL)
}

// paymentsHandler handles requests to /api/payments
func (p *Proxy) paymentsHandler(c *fiber.Ctx) error {
	log.Printf("Routing /api/payments to Monolith")
	return p.proxyRequest(c, p.Config.MonolithURL)
}

// subscriptionsHandler handles requests to /api/subscriptions
func (p *Proxy) subscriptionsHandler(c *fiber.Ctx) error {
	log.Printf("Routing /api/subscriptions to Monolith")
	return p.proxyRequest(c, p.Config.MonolithURL)
}

// eventsHandler handles requests to /api/events
func (p *Proxy) eventsHandler(c *fiber.Ctx) error {
	log.Printf("Routing /api/events to Events Service")
	return p.proxyRequest(c, p.Config.EventsServiceURL)
}

// shouldRouteToMoviesService determines if request should go to movies service
func shouldRouteToMoviesService(percent int) bool {
	return rand.Intn(100) < percent
}

// proxyRequest forwards the request to the target service
func (p *Proxy) proxyRequest(c *fiber.Ctx, targetURL string) error {
	// Build target URI
	targetURI := targetURL + string(c.Request().URI().Path())
	if string(c.Request().URI().QueryString()) != "" {
		targetURI += "?" + string(c.Request().URI().QueryString())
	}

	log.Printf("Proxying %s %s to %s", c.Method(), c.Path(), targetURI)

	// Create request
	req := fasthttp.AcquireRequest()
	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseRequest(req)
	defer fasthttp.ReleaseResponse(resp)

	// Copy request
	req.Header.SetMethod(c.Method())
	req.SetRequestURI(targetURI)

	// Copy headers
	c.Request().Header.VisitAll(func(key, value []byte) {
		req.Header.SetBytesKV(key, value)
	})

	// Copy body
	req.SetBody(c.Body())

	// Send request
	var client *fasthttp.Client
	switch targetURL {
	case p.Config.MonolithURL:
		client = p.Monolith
	case p.Config.MoviesServiceURL:
		client = p.MoviesService
	case p.Config.EventsServiceURL:
		client = p.EventsService
	}

	if err := client.Do(req, resp); err != nil {
		log.Printf("Proxy error: %v", err)
		return c.Status(fiber.StatusBadGateway).JSON(fiber.Map{
			"error": fmt.Sprintf("Failed to proxy request: %v", err),
		})
	}

	// Copy response headers
	resp.Header.VisitAll(func(key, value []byte) {
		c.Response().Header.SetBytesKV(key, value)
	})

	// Set response status and body
	c.Status(resp.StatusCode())
	return c.Send(resp.Body())
}
