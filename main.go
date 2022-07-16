package main

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"mime/multipart"
	"net/http"
	"strings"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

var IMAGE_CACHE_URL = "http://localhost/file_upload/cacheFeatureVector"
var MONGO_DB_SERVER = "mongodb://webcom:webcom@127.0.0.1:27017/webcom"
var THREAD_NUM = 4

var wg sync.WaitGroup
var count = 0

func getItemMap(cursor *mongo.Cursor, ctx context.Context, itemMap *bson.M, mu *sync.Mutex) bool {

	mu.Lock()
	defer mu.Unlock()

	if !cursor.Next(ctx) {
		log.Println("data end")
		return false
	}

	count++
	fmt.Println(count)

	if err := cursor.Decode(itemMap); err != nil {
		log.Fatal(err)
		return false
	}

	return true
}

func sendThumbnailToCache(cursor *mongo.Cursor, ctx context.Context, mu *sync.Mutex) {
	defer wg.Done()

	for {

		var itemMap bson.M
		if !getItemMap(cursor, ctx, &itemMap, mu) {
			return
		}

		imgMap := itemMap["image"].(bson.M)
		currentMap := imgMap["current"].(bson.M)
		thumbnailUrl := currentMap["thumbnail"].(string)

		fmt.Println(time.Now().Local().String() + " GET : " + thumbnailUrl)
		client := http.Client{
			Timeout: 15 * time.Second,
		}
		respGet, err := client.Get(thumbnailUrl)
		if err != nil {
			//log.Fatal(err)
			log.Println("to continue")
			log.Println(err)
			// respGet.Body.Close()
			continue
		}

		var byteArray []byte
		if respGet != nil {
			byteArray, _ = ioutil.ReadAll(respGet.Body)
			respGet.Body.Close()
		}
		body := &bytes.Buffer{}
		mw := multipart.NewWriter(body)

		urlslice := strings.Split(thumbnailUrl, "/")
		filename := urlslice[len(urlslice)-1]

		fw, _ := mw.CreateFormFile("file", filename)

		fw.Write(byteArray)
		err = mw.Close()

		respPost, err := http.Post(IMAGE_CACHE_URL, mw.FormDataContentType(), body)
		if err != nil {
			log.Fatal("post error")
			log.Fatal(err)
			continue
		}

		if respPost != nil {
			if respPost.StatusCode >= 400 {
				log.Println("error : " + respPost.Status)
			}
			respPost.Body.Close()
		}
	}

}

func main() {

	client, err := mongo.Connect(context.TODO(), options.Client().ApplyURI(MONGO_DB_SERVER))

	if err != nil {
		log.Fatal(err)
	}

	defer func() {
		if err = client.Disconnect(context.TODO()); err != nil {
			panic(err)
		}
	}()

	// Ping the primary
	if err := client.Ping(context.TODO(), readpref.Primary()); err != nil {
		panic(err)
	}
	fmt.Println("Successfully connected and pinged.")

	ctx, cancel := context.WithTimeout(context.Background(), 3000*time.Second)
	defer cancel()

	col := client.Database("webcom").Collection("webcom")
	cursor, err := col.Find(ctx, bson.M{})

	if err != nil {
		log.Fatal(err)
	}

	defer cursor.Close(ctx)

	var mu sync.Mutex
	wg.Add(THREAD_NUM)

	for i := 0; i < THREAD_NUM; i++ {
		go sendThumbnailToCache(cursor, ctx, &mu)
	}

	wg.Wait()
	fmt.Println("END")
}
