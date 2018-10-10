package main

import (
	"flag"
	"fmt"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"syscall"

	"github.com/tiglabs/baudengine/util/log"
	"github.com/tiglabs/raft/jepsenraft/raftserver"
	"github.com/tiglabs/raft/logger"
)

var (
	configFile = flag.String("c", "", "config file path")
	mainWg     sync.WaitGroup
)

type IServer interface {
	Start(cfg *raftserver.Config) error
	Shutdown()
}

const (
	CONFIG_LOG_LEVEL_INT_DEBUG = 1
	CONFIG_LOG_LEVEL_INT_INFO  = 2
	CONFIG_LOG_LEVEL_INT_WARN  = 3
	CONFIG_LOG_LEVEL_INT_ERROR = 4
)

func interceptSignal(s IServer) {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		signal := <-sigs
		log.Info("RaftServer received signal[%v]", signal.String())

		s.Shutdown()
		mainWg.Done()
		os.Exit(0)
	}()
}

func main() {
	fmt.Println("Hello, Raft Server!")
	flag.Parse()
	fmt.Printf("Configfile=[%v]\n", *configFile)

	//for multi-cpu scheduling
	runtime.GOMAXPROCS(runtime.NumCPU())

	cfg := raftserver.NewConfig(*configFile)

	log.InitFileLog(cfg.LogCfg.LogPath, "raftserver", cfg.LogCfg.Level)
	raftLog := logger.NewDefaultLogger(CONFIG_LOG_LEVEL_INT_DEBUG)
	raftLog.SetLevel(cfg.LogCfg.RaftLevel)
	logger.SetLogger(raftLog)

	log.Debug("Log has been initialized")

	server := raftserver.NewServer()
	mainWg.Add(1)
	interceptSignal(server)

	err := server.Start(cfg)
	if err != nil {
		log.Fatal("Fatal: failed to start the Master daemon - ", err)
	}

	log.Info("Master is running!")
	mainWg.Wait()
	log.Info("Goodbye, Master!")
}
