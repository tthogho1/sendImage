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
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

func main() {

	client, err := mongo.Connect(context.TODO(), options.Client().ApplyURI("mongodb://webcom:webcom@127.0.0.1:27017/webcom"))

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

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	col := client.Database("webcom").Collection("webcom")
	cursor, err := col.Find(ctx, bson.M{})

	if err != nil {
		log.Fatal(err)
		return
	}

	defer cursor.Close(ctx)

	for cursor.Next(ctx) {
		var itemMap bson.M
		if err = cursor.Decode(&itemMap); err != nil {
			log.Fatal(err)
			return
		}

		imgMap := itemMap["image"].(bson.M)
		currentMap := imgMap["current"].(bson.M)
		thumbnailUrl := currentMap["thumbnail"].(string)

		fmt.Println(thumbnailUrl)

		resp, err := http.Get(thumbnailUrl)
		if err != nil {
			log.Fatal(err)
			return
		}
		defer resp.Body.Close()

		byteArray, _ := ioutil.ReadAll(resp.Body)

		body := &bytes.Buffer{}
		mw := multipart.NewWriter(body)

		urlslice := strings.Split(thumbnailUrl, "/")
		filename := urlslice[len(urlslice)-1]

		fw, _ := mw.CreateFormFile("file", filename)

		//_, err = io.Copy(fw, byteArray)
		fw.Write(byteArray)
		contentType := mw.FormDataContentType()
		err = mw.Close()

		url := "http://localhost:53197/file_upload/cacheFeatureVector"
		resp2, _ := http.Post(url, contentType, body)
		if err != nil {
			log.Fatal(err)
			return
		}

		resp2.Body.Close()
	}
}
