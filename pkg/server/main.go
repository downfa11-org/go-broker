package server

import (
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/downfa11-org/go-broker/pkg/config"
	"github.com/downfa11-org/go-broker/pkg/controller"
	"github.com/downfa11-org/go-broker/pkg/disk"
	"github.com/downfa11-org/go-broker/pkg/metrics"
	"github.com/downfa11-org/go-broker/pkg/topic"
	"github.com/downfa11-org/go-broker/pkg/types"
	"github.com/downfa11-org/go-broker/util"
)

const (
	maxWorkers      = 1000
	readDeadline    = 5 * time.Minute // Read deadline defined as a constant
	HealthCheckPort = 9080            // Port for health check endpoint
)

// RunServer starts the broker with optional TLS and gzip
func RunServer(cfg *config.Config, tm *topic.TopicManager, dm *disk.DiskManager) error {
	startHealthCheckServer(HealthCheckPort)

	if cfg.EnableExporter {
		metrics.StartMetricsServer(cfg.ExporterPort)
		log.Printf("📈 Prometheus exporter started on port %d", cfg.ExporterPort)
	} else {
		log.Println("📉 Exporter disabled")
	}

	addr := fmt.Sprintf(":%d", cfg.BrokerPort)
	var ln net.Listener
	var err error
	if cfg.UseTLS {
		tlsConfig := &tls.Config{Certificates: []tls.Certificate{cfg.TLSCert}}
		ln, err = tls.Listen("tcp", addr, tlsConfig)
	} else {
		ln, err = net.Listen("tcp", addr)
	}
	if err != nil {
		return err
	}

	log.Printf("🧩 Broker listening on %s (TLS=%v, Gzip=%v)", addr, cfg.UseTLS, cfg.EnableGzip)

	workerCh := make(chan net.Conn, maxWorkers)
	for i := 0; i < maxWorkers; i++ {
		go func() {
			for conn := range workerCh {
				HandleConnection(conn, tm, dm, cfg.EnableGzip)
			}
		}()
	}

	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Printf("⚠️ Accept error: %v", err)
			continue
		}
		workerCh <- conn
	}
}

// HandleConnection processes a single client connection
func HandleConnection(conn net.Conn, tm *topic.TopicManager, dm *disk.DiskManager, enableGzip bool) {
	defer conn.Close()

	cmdHandler := controller.NewCommandHandler(tm, dm)
	ctx := controller.NewClientContext("tcp-group", 0)

	for {
		if err := conn.SetReadDeadline(time.Now().Add(readDeadline)); err != nil {
			log.Printf("⚠️ SetReadDeadline error: %v", err)
			return
		}

		lenBuf := make([]byte, 4)
		if _, err := io.ReadFull(conn, lenBuf); err != nil {
			if err != io.EOF {
				log.Printf("⚠️ Read length error: %v", err)
			}
			return
		}

		msgLen := binary.BigEndian.Uint32(lenBuf)
		msgBuf := make([]byte, msgLen)
		if _, err := io.ReadFull(conn, msgBuf); err != nil {
			log.Printf("⚠️ Read message error: %v", err)
			return
		}

		data, err := DecompressMessage(msgBuf, enableGzip)
		if err != nil {
			log.Printf("⚠️ Decompress error: %v", err)
			return
		}

		cmdStr := strings.TrimSpace(string(data))

		if isCommand(cmdStr) {
			resp := cmdHandler.HandleCommand(cmdStr, ctx)
			writeResponse(conn, resp)
			continue
		}

		topicName, payload := util.DecodeMessage(data)
		if topicName == "" || payload == "" {
			return
		}
		tm.Publish(topicName, types.Message{
			Payload: payload,
			Key:     payload,
		})
		if dh, err := dm.GetHandler(topicName, 0); err == nil {
			dh.Log("info", fmt.Sprintf("Received message for topic %s: %s", topicName, payload))
		}

		writeResponse(conn, "OK")
	}
}

func isCommand(s string) bool {
	keywords := []string{"CREATE", "DELETE", "LIST", "SUBSCRIBE", "PUBLISH", "CONSUME", "HELP"}
	for _, k := range keywords {
		if strings.HasPrefix(strings.ToUpper(s), k) {
			return true
		}
	}
	return false
}

func writeResponse(conn net.Conn, msg string) {
	resp := []byte(msg)
	respLen := make([]byte, 4)
	binary.BigEndian.PutUint32(respLen, uint32(len(resp)))

	if _, err := conn.Write(respLen); err != nil {
		log.Printf("⚠️ Write length error: %v", err)
		return
	}
	if _, err := conn.Write(resp); err != nil {
		log.Printf("⚠️ Write response error: %v", err)
		return
	}
}

// startHealthCheckServer starts a simple HTTP server for health checks
func startHealthCheckServer(port int) {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})

	addr := fmt.Sprintf(":%d", port)

	go func() {
		if err := http.ListenAndServe(addr, mux); err != nil {
			log.Printf("❌ Health check server failed: %v", err)
		}
	}()
	log.Printf("🩺 Health check endpoint /health started on port %d", port)
}
