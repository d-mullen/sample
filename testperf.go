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
	"google.golang.org/api/iterator"
)

func ResetEmulator(ctx context.Context, project string) error {
	fsHost := os.Getenv("FIRESTORE_EMULATOR_HOST")
	fmt.Printf("Reset emulator:%s\n", fsHost)
	if fsHost == "" {
		return errors.New("FIRESTORE_EMULATOR_HOST is not set")
	}
	client := &http.Client{}
	r, err := http.NewRequestWithContext(ctx, "DELETE", fmt.Sprintf("http://%s/emulator/v1/projects/%s/databases/(default)/documents", fsHost, project), nil)
	if err != nil {
		return err
	}
	_, err = client.Do(r)
	return err
}

func createClient(ctx context.Context) *firestore.Client {
	projectID := "zing-dev-adjunct"
	//	err := ResetEmulator(ctx, projectID)
	client, err := firestore.NewClient(ctx, projectID)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	return client
}

func addEventsToCollection(ctx context.Context, DocCount int, col *firestore.CollectionRef) (err error) {
	baseTime := time.Now().Add(-7 * time.Hour * 24)
	for i := 0; i < DocCount; i++ {
		uuid_1, _ := uuid.New()
		uuid_2, _ := uuid.New()
		_, _, err = col.Add(ctx, map[string]interface{}{
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
	return err
}

func main() {
	//var DocCount = 10000
	var tenant = "smoke-tenant"
	var ColName = fmt.Sprintf("EventContextTenants/%s/Events", tenant)

	// Get a Firestore client.
	ctx := context.Background()
	client := createClient(ctx)
	defer client.Close()
	log.Println("Created client")

	/*
		collections, err := client.Collections(ctx).GetAll()
		if err != nil {
			log.Printf("%v\n", err)
			log.Fatal("Something wrong with the client")
		}

		for _, c := range collections {
			log.Printf("collection: %s", c.Path)
		}
	*/

	col := client.Collection(ColName)

	if col == nil {
		log.Fatalf("Could not get collection [%s]", ColName)
	}

	log.Printf("Found Collection %s", col.Path)

	// Query the collection.
	start := time.Now()
	q := col.Select("id").OrderBy("createdAt", firestore.Desc)

	docIter := q.Documents(ctx)

	totalDocs := 0
	defer docIter.Stop()
	for {
		doc, done := docIter.Next()
		if done == iterator.Done {
			break
		}
		if done != nil {
			log.Fatalf("Error processing data: %v", done)
		}

		m := doc.Data()
		totalDocs++
		_ = m
		// fmt.Printf("id: %v\n", m["id"])
	}
	end := time.Now()
	secs := end.Sub(start).Seconds()
	fmt.Printf("Query completed in %v seconds.\n", secs)

	log.Printf("Found %d documents", totalDocs)
	fmt.Println("DONE")
}
