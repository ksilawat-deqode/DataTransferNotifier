package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	_ "github.com/lib/pq"
)

type JobDetail struct {
	Id          string `json:"id"`
	JobId       string `json:"jobId"`
	JobStatus   string `json:"jobStatus"`
	RequestId   string `json:"requestId"`
	Query       string `json:"query"`
	Destination string `json:"destination"`
}

var db *sql.DB

func init() {
	host := os.Getenv("DB_HOST")
	port := os.Getenv("DB_PORT")
	user := os.Getenv("DB_USER")
	password := os.Getenv("DB_PASSWORD")
	databaseName := os.Getenv("DB_NAME")

	connection := fmt.Sprintf(
		"host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		host,
		port,
		user,
		password,
		databaseName,
	)
	db, _ = sql.Open("postgres", connection)
}

func main() {
	lambda.Start(HandleEvent)
}

func GetJobDetail(taskExecutionArn string) (JobDetail, error) {
	log.Printf("Initiating GetJobDetail with taskExecutionArn:%v", taskExecutionArn)

	statement := `SELECT id, jobid, jobstatus, requestid, query, destination FROM emr_job_details WHERE task_execution_arn=$1`
	var jobDetail JobDetail

	record := db.QueryRow(statement, taskExecutionArn)

	switch err := record.Scan(
		&jobDetail.Id,
		&jobDetail.JobId,
		&jobDetail.JobStatus,
		&jobDetail.RequestId,
		&jobDetail.Query,
		&jobDetail.Destination,
	); err {
	case sql.ErrNoRows:
		return jobDetail, sql.ErrNoRows
	case nil:
		return jobDetail, nil
	default:
		return jobDetail, err
	}
}

func UpdateJob(jobDetail JobDetail, updatedDataTransferStatus string) {
	log.Printf("%v-> Data Sync: Initiating UpdateJob", jobDetail.Id)

	statement := `UPDATE emr_job_details SET data_transfer_state = $1 WHERE jobid = $2;`

	log.Printf("%v-> Data Sync: Updating record for arnId: %v\n", jobDetail.Id, jobDetail.JobId)

	_, err := db.Exec(statement, updatedDataTransferStatus, jobDetail.JobId)

	if err != nil {
		log.Printf("%v-> Data Sync: Failed to update record for jobId: %v with error: %v\n", jobDetail.Id, jobDetail.JobId, err.Error())
		return
	}

	log.Printf("%v-> Data Sync: Successfully Updated jobStatus: %v for jobId: %v\n", jobDetail.Id, updatedDataTransferStatus, jobDetail.JobId)
}

func HandleEvent(event events.CloudWatchEvent) {
	if event.Source == "aws.datasync" {

		log.Printf("Initiating data sync Notifier\n")

		var eventContext map[string]interface{}
		if err := json.Unmarshal(event.Detail, &eventContext); err != nil {
			log.Printf("Failed to serializer data with error: %v\n", err.Error())
			return
		}
		taskExecutionArn := fmt.Sprint(event.Resources[0])
		updatedDataTransferStatus := fmt.Sprint(eventContext["State"])

		jobDetail, err := GetJobDetail(taskExecutionArn)
		if err != nil {
			log.Printf("Data Sync: Failed to get job details for taskExecutionArn: %v with error: %v\n", taskExecutionArn, err.Error())
			return
		}
		UpdateJob(jobDetail, updatedDataTransferStatus)
	}
}