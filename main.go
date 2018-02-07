package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/lib/pq"
	"github.com/xitongsys/parquet-go/ParquetType"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/xitongsys/parquet-go/ParquetFile"
	"github.com/xitongsys/parquet-go/ParquetReader"
)

func main() {
	lambda.Start(HandleRequest)
}

func HandleRequest(ctx context.Context, s3Event events.S3Event) (events.S3Object, error) {
	// get event record (record of event that triggers lambda)
	rec := s3Event.Records[0]

	// reading bucket/filename from record
	bucket := rec.S3.Bucket.Name
	filename := rec.S3.Object.Key
	fmt.Println("Bucket", bucket, "Filename", filename)
	tmpfilepath := fmt.Sprintf("/tmp/%s", filename)

	// Creating temporary file to download s3 file to
	f, err := os.Create(tmpfilepath)
	if err != nil {
		fmt.Errorf("failed to create file %q, %v", filename, err)
	}

	// Creating aws session
	sess, _ := session.NewSession(&aws.Config{
		Region: aws.String("us-east-1")},
	)
	// get Credentials
	_, err = sess.Config.Credentials.Get()
	if err != nil {
		fmt.Errorf("Credentials failed, %v", err)
	}

	// Create downloader and download s3 file
	downloader := s3manager.NewDownloader(sess)
	fmt.Printf("Downloading %s/%s\n", bucket, filename)
	n, err := downloader.Download(f, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(filename),
	})
	if err != nil {
		fmt.Errorf("failed to download file, %v", err)
	}

	fmt.Printf("file downloaded, %d bytes\n", n)

	readParquetFile(tmpfilepath)

	return rec.S3.Object, err
}

func readParquetFile(filepath string) {

	if !strings.HasSuffix(filepath, ".parquet") {
		log.Fatalln("Only capable of parsing parquet files")
	}

	var orderIDs, firstNames, lastNames, emails, quantities, orderTotals []interface{}

	fr, err := ParquetFile.NewLocalFileReader(filepath)
	defer fr.Close()
	if err != nil {
		log.Fatalln("Unable to open file", err)
	}
	pr, err := ParquetReader.NewParquetColumnReader(fr, 4)
	defer pr.ReadStop()
	if err != nil {
		log.Fatalln("Unable to create column reader")
	}
	rows := int(pr.GetNumRows())

	// ReadColumnByPath returns table values, repetition levels, and definition levels
	// I used index instead of path on some of these because the parquet-go library was a bit buggy
	orderIDs, _, _ = pr.ReadColumnByIndex(0, rows)
	firstNames, _, _ = pr.ReadColumnByPath("FirstName", rows)
	lastNames, _, _ = pr.ReadColumnByPath("LastName", rows)
	emails, _, _ = pr.ReadColumnByPath("Email", rows)
	quantities, _, _ = pr.ReadColumnByIndex(4, rows)
	orderTotals, _, _ = pr.ReadColumnByIndex(5, rows)

	orders := make([]Order, 0, rows)
	for i := 0; i < rows; i++ {
		orders = append(orders, Order{
			OrderID:    int32(orderIDs[i].(ParquetType.INT32)),
			FirstName:  string(firstNames[i].(ParquetType.BYTE_ARRAY)),
			LastName:   string(lastNames[i].(ParquetType.BYTE_ARRAY)),
			Email:      string(emails[i].(ParquetType.BYTE_ARRAY)),
			Quantity:   int32(quantities[i].(ParquetType.INT32)),
			OrderTotal: float64(orderTotals[i].(ParquetType.DOUBLE)),
		})
	}
	sendOrdersToDataBase(orders)
}

func sendOrdersToDataBase(orders []Order) {

	host := os.Getenv("HOST")
	port, err := strconv.Atoi(os.Getenv("PORT"))
	user := os.Getenv("rdsuser")
	password := os.Getenv("rdspassword")
	dbname := os.Getenv("rdsdb")

	psqlInfo := fmt.Sprintf("host=%s port=%d user=%s "+
		"password=%s dbname=%s sslmode=disable",
		host, port, user, password, dbname)

	db, err := sql.Open("postgres", psqlInfo)
	defer db.Close()
	if err != nil {
		log.Fatal(err)
	}

	txn, err := db.Begin()
	if err != nil {
		log.Fatal(err)
	}

	stmt, err := txn.Prepare(pq.CopyIn("test_orders", "orderid", "firstname", "lastname", "email", "quantity", "ordertotal"))
	if err != nil {
		log.Fatalln(err)
	}

	for _, order := range orders {
		_, err = stmt.Exec(order.OrderID, order.FirstName, order.LastName, order.Email, order.Quantity, order.OrderTotal)
	}
	if err != nil {
		log.Fatal(err)
	}

	_, err = stmt.Exec()
	if err != nil {
		log.Fatal(err)
	}

	err = stmt.Close()
	if err != nil {
		log.Fatal(err)
	}

	err = txn.Commit()
	if err != nil {
		log.Fatal(err)
	}
}

type Order struct {
	OrderID    int32
	FirstName  string
	LastName   string
	Email      string
	Quantity   int32
	OrderTotal float64
}
