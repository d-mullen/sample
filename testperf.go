package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"time"

	"cloud.google.com/go/firestore"
	"go.mongodb.org/mongo-driver/x/mongo/driver/uuid"
)

func resetFB(ctx context.Context, project string) error {
	fsHost := os.Getenv("FIRESTORE_EMULATOR_HOST")
	fmt.Printf("Reset emulator:%s\n", fsHost)
	if fsHost == "" {
		return errors.New("FIRESTORE_EMULATOR_HOST is not set")
	}
	client := &http.Client{}
	r, err := http.NewRequestWithContext(ctx, http.MethodDelete, fmt.Sprintf("http://%s/emulator/v1/projects/%s/databases/(default)/documents", fsHost, project), nil)
	if err != nil {
		return err
	}
	_, err = client.Do(r)
	return err
}

func createClient(ctx context.Context) *firestore.Client {
	projectID := "dev"
	err := resetFB(ctx, projectID)
	client, err := firestore.NewClient(ctx, projectID)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	return client
}

func main() {
	var DocCount = 10000
	var ColName = "perftesting"

	// Get a Firestore client.
	ctx := context.Background()
	client := createClient(ctx)
	defer client.Close()
	fmt.Println("Created client")
	// [START firestore_setup_dataset_pt1]
	col := client.Collection(ColName)
	fmt.Println("Created collection")
	baseTime := time.Now().Add(-7 * time.Hour * 24)
	for i := 0; i < DocCount; i++ {
		uuid_1, _ := uuid.New()
		uuid_2, _ := uuid.New()
		_, _, err := col.Add(ctx, map[string]interface{}{
			"_id":       fmt.Sprintf("%X", uuid_1),
			"EventID":   fmt.Sprintf("%X", uuid_2),
			"TenantID":  rand.Intn(5),
			"timestamp": baseTime.UnixNano() / 1e6,
		})
		if err != nil {
			fmt.Println("Failed to add")
		}

		baseTime = baseTime.Add(10 * time.Second)
	}
	fmt.Printf("Inserted %d documents\n", DocCount)

	// Query the collection.
	start := time.Now()
	q := col.Select("EventID").Where("TenantID", "==", 1).
		OrderBy("timestamp", firestore.Desc).Limit(2000)
	docIter, err := q.Documents(ctx).GetAll()

	if err != nil {
		log.Fatalf("Failed to get docs, %v", err)
	}

	end := time.Now()
	totalDocs := 0
	for _, _ = range docIter {
		// m := doc.Data()
		totalDocs++
		//	fmt.Printf("EventID: %v\n", m["EventID"])
	}
	secs := end.Sub(start).Seconds()
	fmt.Printf("Found %d doc in %v seconds.\n", totalDocs, secs)
	fmt.Println("DONE")
}
