package main

import (
	"context"
	"dysco"
	"dysco/netFileStream"
	"fmt"
	"os"
	"os/signal"
	"strconv"

	//	tcmu "github.com/coreos/go-tcmu"
	log "github.com/sirupsen/logrus"
)

func main() {
	log.SetLevel(log.DebugLevel)
	log.SetOutput(os.Stderr)

	if len(os.Args) < 6 {
		die("not enough arguments")
	}

	name := os.Args[2]
	container := os.Args[3]
	sa := os.Args[4]
	sak := os.Args[5]

	switch os.Args[1] {
	case "-c":
		blocks, err := strconv.ParseInt(os.Args[6], 10, 64)
		if err != nil {
			die("Failure creating page blob: ", err.Error())
		}
		_, err = dysco.CreatePageBlob(name, blocks, container, sa, sak)
		if err != nil {
			die("Failure creating page blob: ", err.Error())
		}
		os.Exit(0)
	}

	ctx := context.Background()

	_, err := netFileStream.NewNetFileVolume(name, ctx)
	if err != nil {
		die("Failure opening page blob: ", err.Error())
	}
	/*
		v, err := dysco.OpenPageBlob(name, container, sa, sak)
		if err != nil {
			die("Failure opening page blob: ", err.Error())
		}

			//--------
			// TCMU hooks
			handler := tcmu.BasicSCSIHandler(v)
			handler.VolumeName = name
			handler.DataSizes.VolumeSize = v.GetSize()
			d, err := tcmu.OpenTCMUDevice("/dev/tcmufile", handler)
			if err != nil {
				die("couldn't tcmu: %v", err)
			}
			defer d.Close()
			fmt.Printf("go-tcmu attached to %s/%s\n", "/dev/tcmufile", name)
	*/
	mainClose := make(chan bool)
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)

	go func() {
		for _ = range signalChan {
			fmt.Println("\nReceived an interrupt, stopping services...")
			close(mainClose)
		}
	}()
	<-mainClose
}

func die(why string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, why+"\n", args...)
	os.Exit(1)
}
