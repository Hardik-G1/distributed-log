package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"strings"

	"github.com/Hardik-G1/distributed-log/internal/storage"
)

func main() {
	appDirectory := flag.String(
		"app-data-directory",
		"",
		"application-state directory",
	)
	resourcePrefix := flag.String(
		"resource-prefix",
		"resource",
		"resource ID prefix",
	)
	resourceCount := flag.Int(
		"resource-count",
		1000,
		"number of resources",
	)
	flag.Parse()

	if strings.TrimSpace(*appDirectory) == "" {
		log.Fatal("application data directory is required")
	}
	if strings.TrimSpace(*resourcePrefix) == "" {
		log.Fatal("resource prefix is required")
	}
	if *resourceCount <= 0 {
		log.Fatal(errors.New("resource count must be positive"))
	}

	store, err := storage.OpenMemoryApplicationStore(*appDirectory)
	if err != nil {
		log.Fatal(err)
	}
	defer store.Close()

	resources := make([]storage.ResourceLocation, 0, *resourceCount)

	for index := 0; index < *resourceCount; index++ {
		resourceID := fmt.Sprintf("%s-%d", *resourcePrefix, index)

		resources = append(resources, storage.ResourceLocation{
			ResourceID: resourceID,
			Location:   "memory://" + resourceID,
		})
	}

	if err := store.SaveResourceLocations(
		context.Background(),
		resources,
	); err != nil {
		log.Fatal(err)
	}

	log.Printf(
		"initialized %d resources in %s",
		len(resources),
		*appDirectory,
	)
}
