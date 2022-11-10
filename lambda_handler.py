def lambda_handler(event, context):
    if event["source"] == "aws.datasync":
        task_execution_arn = event["resources"][0]
        data_transfer_state = event["detail"]["State"]

        print(f"task_execution_arn: {task_execution_arn}")
        print(f"data_transfer_state: {data_transfer_state}")
