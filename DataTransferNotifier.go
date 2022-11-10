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
	Id                 string `json:"id"`
	JobId              string `json:"jobId"`
	JobStatus          string `json:"jobStatus"`
	RequestId          string `json:"requestId"`
	Query              string `json:"query"`
	Destination        string `json:"destination"`
	TaskExecutionArn   string `json:"taskExecutionArn"`
	DataTransferStatus string `json:"dataTransferStatus"`
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

	statement := `SELECT id, jobid, jobstatus, requestid, query, destination, task_execution_arn, data_transfer_state FROM emr_job_details WHERE task_execution_arn=$1`
	var jobDetail JobDetail

	record := db.QueryRow(statement, taskExecutionArn)

	switch err := record.Scan(
		&jobDetail.Id,
		&jobDetail.JobId,
		&jobDetail.JobStatus,
		&jobDetail.RequestId,
		&jobDetail.Query,
		&jobDetail.Destination,
		&jobDetail.TaskExecutionArn,
		&jobDetail.DataTransferStatus,
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

	statement := `UPDATE emr_job_details SET data_transfer_state = $1 WHERE id = $2;`

	log.Printf("%v-> Data Sync: Updating record for taskExecutionArn: %v\n", jobDetail.Id, jobDetail.TaskExecutionArn)

	_, err := db.Exec(statement, updatedDataTransferStatus, jobDetail.Id)

	if err != nil {
		log.Printf("%v-> Data Sync: Failed to update record for taskExecutionArn: %v with error: %v\n", jobDetail.Id, jobDetail.TaskExecutionArn, err.Error())
		return
	}

	log.Printf("%v-> Data Sync: Successfully Updated dataTransferStatus: %v for taskExecutionArn: %v\n", jobDetail.Id, updatedDataTransferStatus, jobDetail.TaskExecutionArn)

	if updatedDataTransferStatus == "SUCCESS" {
		statement := `UPDATE emr_job_details SET jobstatus = $1 WHERE id = $2;`

		log.Printf("%v-> Data Sync: Updating record for id: %v\n", jobDetail.Id, jobDetail.Id)

		_, err := db.Exec(statement, updatedDataTransferStatus, jobDetail.Id)

		if err != nil {
			log.Printf("%v-> Data Sync: Failed to update record for id: %v with error: %v\n", jobDetail.Id, jobDetail.Id, err.Error())
			return
		}

		log.Printf("%v-> Data Sync: Successfully Updated jobstatus: %v for id: %v\n", jobDetail.Id, updatedDataTransferStatus, jobDetail.Id)
	}
}

func HandleEvent(event events.CloudWatchEvent) {
	if event.Source == "aws.datasync" {

		log.Printf("Initiating Data Transfer Notifier\n")

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
