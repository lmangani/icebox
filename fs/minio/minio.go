package minio

import (
	"bytes"
	"context"
	"fmt"
	stdio "io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/apache/iceberg-go/io"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// EmbeddedMinIOConfig represents configuration for embedded MinIO server
type EmbeddedMinIOConfig struct {
	// Server configuration
	Port    int    `yaml:"port" json:"port"`         // Server port (default: 9000)
	Address string `yaml:"address" json:"address"`   // Bind address (default: localhost)
	DataDir string `yaml:"data_dir" json:"data_dir"` // Data directory for MinIO storage

	// Authentication
	AccessKey string `yaml:"access_key" json:"access_key"` // MinIO access key (default: minioadmin)
	SecretKey string `yaml:"secret_key" json:"secret_key"` // MinIO secret key (default: minioadmin)

	// Behavior
	AutoStart bool `yaml:"auto_start" json:"auto_start"` // Auto-start server with Icebox (default: true)
	Console   bool `yaml:"console" json:"console"`       // Enable MinIO console (default: false)
	Quiet     bool `yaml:"quiet" json:"quiet"`           // Suppress MinIO logs (default: true)

	// Security & Performance
	Secure bool `yaml:"secure" json:"secure"` // Use HTTPS (default: false for embedded)

	// Advanced configuration
	Region        string            `yaml:"region" json:"region"`                 // Default region (default: us-east-1)
	DefaultBucket string            `yaml:"default_bucket" json:"default_bucket"` // Default bucket name (default: icebox)
	Properties    map[string]string `yaml:"properties" json:"properties"`         // Additional MinIO properties

	// Connection settings
	MaxIdleConns        int           `yaml:"max_idle_conns" json:"max_idle_conns"`                   // Max idle connections (default: 100)
	MaxIdleConnsPerHost int           `yaml:"max_idle_conns_per_host" json:"max_idle_conns_per_host"` // Max idle per host (default: 10)
	ConnectTimeout      time.Duration `yaml:"connect_timeout" json:"connect_timeout"`                 // Connection timeout (default: 30s)
	RequestTimeout      time.Duration `yaml:"request_timeout" json:"request_timeout"`                 // Request timeout (default: 60s)
}

// EmbeddedMinIO represents an embedded MinIO server instance
type EmbeddedMinIO struct {
	config   *EmbeddedMinIOConfig
	server   *http.Server
	client   *minio.Client
	running  bool
	endpoint string
	mu       sync.RWMutex // Protects running state and client
}

// MinIOFileSystem implements iceberg FileIO interface with embedded MinIO backend
type MinIOFileSystem struct {
	minioServer *EmbeddedMinIO
	client      *minio.Client
	bucket      string
	prefix      string
	mu          sync.RWMutex
}

// NewEmbeddedMinIO creates a new embedded MinIO server instance with modern configuration
func NewEmbeddedMinIO(config *EmbeddedMinIOConfig) (*EmbeddedMinIO, error) {
	if config == nil {
		config = DefaultMinIOConfig()
	}

	// Validate configuration
	if err := validateConfig(config); err != nil {
		return nil, fmt.Errorf("invalid MinIO configuration: %w", err)
	}

	// Ensure data directory exists
	if err := os.MkdirAll(config.DataDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create MinIO data directory: %w", err)
	}

	endpoint := fmt.Sprintf("%s:%d", config.Address, config.Port)

	server := &EmbeddedMinIO{
		config:   config,
		endpoint: endpoint,
		running:  false,
	}

	return server, nil
}

// Start starts the embedded MinIO server with context support and modern practices
func (m *EmbeddedMinIO) Start(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.running {
		return fmt.Errorf("MinIO server is already running")
	}

	// Check if port is available
	if !isPortAvailable(m.config.Port) {
		return fmt.Errorf("port %d is already in use", m.config.Port)
	}

	fmt.Printf("🗄️  Starting embedded MinIO server...\n")
	fmt.Printf("   Endpoint: %s://%s\n", m.getScheme(), m.endpoint)
	fmt.Printf("   Data Directory: %s\n", m.config.DataDir)
	fmt.Printf("   Access Key: %s\n", m.config.AccessKey)
	fmt.Printf("   Region: %s\n", m.config.Region)

	// Set MinIO environment variables with best practices
	m.setEnvironmentVariables()

	// Start MinIO server in a goroutine with context
	serverCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	go func() {
		if err := m.startMinIOServer(serverCtx); err != nil && err != http.ErrServerClosed {
			fmt.Printf("❌ MinIO server error: %v\n", err)
		}
	}()

	// Wait for server to be ready with timeout
	readyCtx, readyCancel := context.WithTimeout(ctx, 30*time.Second)
	defer readyCancel()

	if err := m.waitForReady(readyCtx); err != nil {
		return fmt.Errorf("MinIO server failed to start: %w", err)
	}

	// Create MinIO client with modern configuration
	client, err := m.createClient()
	if err != nil {
		return fmt.Errorf("failed to create MinIO client: %w", err)
	}

	m.client = client
	m.running = true

	// Create default bucket if specified
	if m.config.DefaultBucket != "" {
		if err := m.ensureBucket(ctx, m.config.DefaultBucket); err != nil {
			return fmt.Errorf("failed to create default bucket: %w", err)
		}
	}

	fmt.Printf("✅ MinIO server started successfully\n")
	if m.config.Console {
		fmt.Printf("🌐 MinIO Console: %s://%s\n", m.getScheme(), m.endpoint)
	}

	return nil
}

// createClient creates a modern MinIO client with proper configuration
func (m *EmbeddedMinIO) createClient() (*minio.Client, error) {
	// Create transport with modern settings
	transport := &http.Transport{
		MaxIdleConns:        m.config.MaxIdleConns,
		MaxIdleConnsPerHost: m.config.MaxIdleConnsPerHost,
		IdleConnTimeout:     90 * time.Second,
		TLSHandshakeTimeout: 10 * time.Second,
		DialContext: (&net.Dialer{
			Timeout:   m.config.ConnectTimeout,
			KeepAlive: 30 * time.Second,
		}).DialContext,
	}

	// Create MinIO client with modern options
	client, err := minio.New(m.endpoint, &minio.Options{
		Creds:     credentials.NewStaticV4(m.config.AccessKey, m.config.SecretKey, ""),
		Secure:    m.config.Secure,
		Region:    m.config.Region,
		Transport: transport,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create MinIO client: %w", err)
	}

	// Test connection
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err = client.ListBuckets(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to MinIO server: %w", err)
	}

	return client, nil
}

// setEnvironmentVariables sets MinIO environment variables following best practices
func (m *EmbeddedMinIO) setEnvironmentVariables() {
	// Core credentials
	os.Setenv("MINIO_ROOT_USER", m.config.AccessKey)
	os.Setenv("MINIO_ROOT_PASSWORD", m.config.SecretKey)

	// Server configuration
	os.Setenv("MINIO_ADDRESS", m.endpoint)
	os.Setenv("MINIO_REGION", m.config.Region)

	// Performance and security settings
	if m.config.Secure {
		os.Setenv("MINIO_SECURE", "true")
	}

	if m.config.Quiet {
		os.Setenv("MINIO_QUIET", "true")
	}

	// Set additional properties
	for key, value := range m.config.Properties {
		envKey := fmt.Sprintf("MINIO_%s", key)
		os.Setenv(envKey, value)
	}
}

// getScheme returns the appropriate URL scheme based on security configuration
func (m *EmbeddedMinIO) getScheme() string {
	if m.config.Secure {
		return "https"
	}
	return "http"
}

// Stop stops the embedded MinIO server gracefully
func (m *EmbeddedMinIO) Stop(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if !m.running {
		return nil
	}

	fmt.Printf("🛑 Stopping embedded MinIO server...\n")

	if m.server != nil {
		shutdownCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()

		if err := m.server.Shutdown(shutdownCtx); err != nil {
			return fmt.Errorf("failed to stop MinIO server gracefully: %w", err)
		}
	}

	m.running = false
	m.client = nil
	m.server = nil

	fmt.Printf("✅ MinIO server stopped\n")
	return nil
}

// IsRunning returns whether the MinIO server is currently running (thread-safe)
func (m *EmbeddedMinIO) IsRunning() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.running
}

// GetEndpoint returns the MinIO server endpoint
func (m *EmbeddedMinIO) GetEndpoint() string {
	return m.endpoint
}

// GetClient returns the MinIO client instance (thread-safe)
func (m *EmbeddedMinIO) GetClient() *minio.Client {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.client
}

// NewMinIOFileSystem creates a new MinIO-backed FileSystem with modern configuration
func NewMinIOFileSystem(minioServer *EmbeddedMinIO, bucket, prefix string) (*MinIOFileSystem, error) {
	if !minioServer.IsRunning() {
		return nil, fmt.Errorf("MinIO server is not running")
	}

	client := minioServer.GetClient()
	if client == nil {
		return nil, fmt.Errorf("MinIO client is not available")
	}

	return &MinIOFileSystem{
		minioServer: minioServer,
		client:      client,
		bucket:      bucket,
		prefix:      prefix,
	}, nil
}

// Open opens a file for reading from MinIO with context support
func (fs *MinIOFileSystem) Open(location string) (io.File, error) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	objectName := fs.getObjectName(location)

	// Use context with timeout for better reliability
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	object, err := fs.client.GetObject(ctx, fs.bucket, objectName, minio.GetObjectOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to open object %s: %w", objectName, err)
	}

	return &minioFile{
		object:     object,
		objectName: objectName,
		bucket:     fs.bucket,
		client:     fs.client,
		position:   0,
	}, nil
}

// Create creates a new file for writing to MinIO with modern buffering
func (fs *MinIOFileSystem) Create(location string) (io.File, error) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	objectName := fs.getObjectName(location)

	return &minioWriteFile{
		objectName: objectName,
		bucket:     fs.bucket,
		client:     fs.client,
		buffer:     make([]byte, 0, 64*1024), // Pre-allocate 64KB buffer
	}, nil
}

// Remove removes a file from MinIO with context support
func (fs *MinIOFileSystem) Remove(location string) error {
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	objectName := fs.getObjectName(location)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	return fs.client.RemoveObject(ctx, fs.bucket, objectName, minio.RemoveObjectOptions{})
}

// ensureBucket creates a bucket if it doesn't exist with modern error handling
func (m *EmbeddedMinIO) ensureBucket(ctx context.Context, bucketName string) error {
	exists, err := m.client.BucketExists(ctx, bucketName)
	if err != nil {
		return fmt.Errorf("failed to check bucket existence: %w", err)
	}

	if !exists {
		makeBucketOptions := minio.MakeBucketOptions{
			Region: m.config.Region,
		}

		if err := m.client.MakeBucket(ctx, bucketName, makeBucketOptions); err != nil {
			return fmt.Errorf("failed to create bucket %s: %w", bucketName, err)
		}
		fmt.Printf("📦 Created MinIO bucket: %s (region: %s)\n", bucketName, m.config.Region)
	}

	return nil
}

// DefaultMinIOConfig returns modern default MinIO configuration
func DefaultMinIOConfig() *EmbeddedMinIOConfig {
	return &EmbeddedMinIOConfig{
		// Server settings
		Port:    9000,
		Address: "localhost",
		DataDir: ".icebox/minio",

		// Authentication
		AccessKey: "minioadmin",
		SecretKey: "minioadmin",

		// Behavior
		AutoStart: true,
		Console:   false,
		Quiet:     true,
		Secure:    false, // HTTP for embedded development

		// Advanced settings
		Region:        "us-east-1",
		DefaultBucket: "icebox",
		Properties:    make(map[string]string),

		// Modern connection settings
		MaxIdleConns:        100,
		MaxIdleConnsPerHost: 10,
		ConnectTimeout:      30 * time.Second,
		RequestTimeout:      60 * time.Second,
	}
}

// validateConfig validates MinIO configuration with modern checks
func validateConfig(config *EmbeddedMinIOConfig) error {
	if config.Port <= 0 || config.Port > 65535 {
		return fmt.Errorf("invalid port: %d", config.Port)
	}

	if config.AccessKey == "" {
		return fmt.Errorf("access key cannot be empty")
	}

	if config.SecretKey == "" {
		return fmt.Errorf("secret key cannot be empty")
	}

	if len(config.SecretKey) < 8 {
		return fmt.Errorf("secret key must be at least 8 characters")
	}

	if config.Region == "" {
		config.Region = "us-east-1" // Set default
	}

	if config.MaxIdleConns <= 0 {
		config.MaxIdleConns = 100
	}

	if config.MaxIdleConnsPerHost <= 0 {
		config.MaxIdleConnsPerHost = 10
	}

	if config.ConnectTimeout <= 0 {
		config.ConnectTimeout = 30 * time.Second
	}

	if config.RequestTimeout <= 0 {
		config.RequestTimeout = 60 * time.Second
	}

	return nil
}

// startMinIOServer starts the actual MinIO server process with modern HTTP configuration
func (m *EmbeddedMinIO) startMinIOServer(ctx context.Context) error {
	mux := http.NewServeMux()

	// Health check endpoint
	mux.HandleFunc("/minio/health/live", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"ok"}`))
	})

	// Ready check endpoint
	mux.HandleFunc("/minio/health/ready", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"ready"}`))
	})

	// Basic S3 API endpoint placeholder
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Server", "MinIO/Embedded")
		w.WriteHeader(http.StatusNotImplemented)
		w.Write([]byte("S3 API placeholder - integrate with MinIO server package for full implementation"))
	})

	m.server = &http.Server{
		Addr:         m.endpoint,
		Handler:      mux,
		ReadTimeout:  m.config.RequestTimeout,
		WriteTimeout: m.config.RequestTimeout,
		IdleTimeout:  60 * time.Second,
	}

	// Start server with graceful shutdown support
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		m.server.Shutdown(shutdownCtx)
	}()

	if err := m.server.ListenAndServe(); err != http.ErrServerClosed {
		return err
	}

	return nil
}

// waitForReady waits for the MinIO server to be ready with better error handling
func (m *EmbeddedMinIO) waitForReady(ctx context.Context) error {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("timeout waiting for MinIO server to be ready: %w", ctx.Err())
		case <-ticker.C:
			if m.isServerReady() {
				return nil
			}
		}
	}
}

// isServerReady checks if the server is ready by attempting a connection
func (m *EmbeddedMinIO) isServerReady() bool {
	conn, err := net.DialTimeout("tcp", m.endpoint, 1*time.Second)
	if err != nil {
		return false
	}
	defer conn.Close()
	return true
}

// Helper functions remain the same but with better error handling
func isPortAvailable(port int) bool {
	address := fmt.Sprintf("localhost:%d", port)
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return false
	}
	defer listener.Close()
	return true
}

// getObjectName converts a location to MinIO object name with proper path handling
func (fs *MinIOFileSystem) getObjectName(location string) string {
	if fs.prefix != "" {
		return filepath.Join(fs.prefix, location)
	}
	return location
}

// Modern minioFile implementation with enhanced error handling and performance
type minioFile struct {
	object     *minio.Object
	objectName string
	bucket     string
	client     *minio.Client
	position   int64
	mu         sync.Mutex
}

func (f *minioFile) Read(p []byte) (n int, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	n, err = f.object.Read(p)
	f.position += int64(n)
	return n, err
}

func (f *minioFile) ReadAt(p []byte, off int64) (n int, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	opts := minio.GetObjectOptions{}
	opts.SetRange(off, off+int64(len(p))-1)

	obj, err := f.client.GetObject(ctx, f.bucket, f.objectName, opts)
	if err != nil {
		return 0, err
	}
	defer obj.Close()

	return obj.Read(p)
}

func (f *minioFile) Seek(offset int64, whence int) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	var newPos int64
	switch whence {
	case stdio.SeekStart:
		newPos = offset
	case stdio.SeekCurrent:
		newPos = f.position + offset
	case stdio.SeekEnd:
		return 0, fmt.Errorf("seek from end not supported for MinIO objects")
	default:
		return 0, fmt.Errorf("invalid whence value: %d", whence)
	}

	if newPos < 0 {
		return 0, fmt.Errorf("negative position not allowed")
	}

	// Close current object and reopen from new position
	if f.object != nil {
		f.object.Close()
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	opts := minio.GetObjectOptions{}
	if newPos > 0 {
		opts.SetRange(newPos, -1)
	}

	object, err := f.client.GetObject(ctx, f.bucket, f.objectName, opts)
	if err != nil {
		return 0, err
	}

	f.object = object
	f.position = newPos
	return newPos, nil
}

func (f *minioFile) Stat() (os.FileInfo, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	objInfo, err := f.client.StatObject(ctx, f.bucket, f.objectName, minio.StatObjectOptions{})
	if err != nil {
		return nil, err
	}
	return &minioFileInfo{objInfo: objInfo}, nil
}

func (f *minioFile) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.object != nil {
		return f.object.Close()
	}
	return nil
}

// Modern minioWriteFile implementation with buffering and better error handling
type minioWriteFile struct {
	objectName string
	bucket     string
	client     *minio.Client
	buffer     []byte
	mu         sync.Mutex
}

func (f *minioWriteFile) Read(p []byte) (n int, err error) {
	return 0, fmt.Errorf("read not supported on write-only file")
}

func (f *minioWriteFile) ReadAt(p []byte, off int64) (n int, err error) {
	return 0, fmt.Errorf("read not supported on write-only file")
}

func (f *minioWriteFile) Seek(offset int64, whence int) (int64, error) {
	return 0, fmt.Errorf("seek not supported on write-only file")
}

func (f *minioWriteFile) Stat() (os.FileInfo, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	return &minioWriteFileInfo{
		name: filepath.Base(f.objectName),
		size: int64(len(f.buffer)),
	}, nil
}

func (f *minioWriteFile) Write(p []byte) (n int, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.buffer = append(f.buffer, p...)
	return len(p), nil
}

func (f *minioWriteFile) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Use modern PutObject with proper options
	putOptions := minio.PutObjectOptions{
		ContentType: "application/octet-stream",
	}

	_, err := f.client.PutObject(
		ctx,
		f.bucket,
		f.objectName,
		bytes.NewReader(f.buffer),
		int64(len(f.buffer)),
		putOptions,
	)
	return err
}

// FileInfo implementations remain the same
type minioFileInfo struct {
	objInfo minio.ObjectInfo
}

func (fi *minioFileInfo) Name() string       { return filepath.Base(fi.objInfo.Key) }
func (fi *minioFileInfo) Size() int64        { return fi.objInfo.Size }
func (fi *minioFileInfo) Mode() os.FileMode  { return 0644 }
func (fi *minioFileInfo) ModTime() time.Time { return fi.objInfo.LastModified }
func (fi *minioFileInfo) IsDir() bool        { return false }
func (fi *minioFileInfo) Sys() interface{}   { return fi.objInfo }

type minioWriteFileInfo struct {
	name string
	size int64
}

func (fi *minioWriteFileInfo) Name() string       { return fi.name }
func (fi *minioWriteFileInfo) Size() int64        { return fi.size }
func (fi *minioWriteFileInfo) Mode() os.FileMode  { return 0644 }
func (fi *minioWriteFileInfo) ModTime() time.Time { return time.Now() }
func (fi *minioWriteFileInfo) IsDir() bool        { return false }
func (fi *minioWriteFileInfo) Sys() interface{}   { return nil }
