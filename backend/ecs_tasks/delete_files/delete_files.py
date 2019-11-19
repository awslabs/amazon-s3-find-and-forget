import json
import os
import time
import boto3

# SF are not integrated yet. Waiting for VPC endpoint.
# stepfunctions = boto3.client("stepfunctions")

sqs_endpoint = "https://sqs.{}.amazonaws.com".format(os.getenv("REGION"))
sqs = boto3.resource(service_name='sqs', endpoint_url=sqs_endpoint)
queue = sqs.Queue(os.getenv("DELETE_OBJECTS_QUEUE"))

if __name__ == '__main__':
    while 1:
        print("Fetchine messages...")
        messages = queue.receive_messages(WaitTimeSeconds=5, MaxNumberOfMessages=1)
        if len(messages) == 0:
            print("No messages. Sleeping")
            time.sleep(30)
        else:
            for message in messages:
                print("Message received: {0}".format(message.body))
                body = json.loads(message.body)
                # stepfunctions.send_task_success(taskToken=body["TaskToken"], output=json.dumps({
                #     "Object": body["Input"]["Object"],
                #     "Deletions": 1,
                # }))
                message.delete()
