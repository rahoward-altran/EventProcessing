import boto3
import json
import csv
import logging
import threading
import concurrent.futures
from statistics import mean
from datetime import datetime, timedelta
from SensorMap import SensorMap

LOCATION_FILENAME_W = 'locations_part3.json'
EP_INFO_FILENAME = 'EP_info_part3.json'
# LOCATION_FILENAME_W = 'locations.json'
# EP_INFO_FILENAME = 'EP_info_part1_2.json'
DATA_COLLECTING_TIME_SEC = 5
MAX_SIZED_STORED = 750
MAX_WORKERS = 3

thread_local = threading.local()


def load_json(filename):
    with open(filename) as json_file:
        return json.load(json_file)


if __name__ == "__main__":
    # SET UP LOGGING:
    logging.basicConfig(filename='Log.log', filemode='w', level=logging.INFO)
    logging.info("Program has started and the log is open")

    # READ EP_info.json
    details = load_json(EP_INFO_FILENAME)
    logging.info("%s has been read" % EP_INFO_FILENAME)


    def create_queue(sqs_cli, topic_arn):
        queue_url = sqs_cli.create_queue(
            QueueName='event_notification_queue'
        )['QueueUrl']
        queue_arn = sqs_cli.get_queue_attributes(
            QueueUrl=queue_url,
            AttributeNames=['QueueArn']
        )['Attributes']['QueueArn']


        def get_policy(topic_arn, queue_arn):
            policy_document = {
                'Version': '2012-10-17',
                'Statement': [{
                    'Sid': f'allow-subscription-{topic_arn}',
                    'Effect': 'Allow',
                    'Principal': {'AWS': '*'},
                    'Action': 'SQS:SendMessage',
                    'Resource': f'{queue_arn}',
                    'Condition': {
                        'ArnEquals': {'aws:SourceArn': f'{topic_arn}'}
                    }
                }]
            }
            return json.dumps(policy_document)


        policy = get_policy(topic_arn, queue_arn)
        queue = boto3.resource('sqs').Queue(queue_url)
        queue.set_attributes(
            Attributes={
                'Policy': policy
            }
        )
        return queue_url, queue_arn


    # CREATE QUEUE - event_notification_queue
    sqs = boto3.client('sqs')
    topic_arn = details['SNS']['Arn']
    queue_url, queue_arn = create_queue(sqs, topic_arn)
    logging.info("queue created")


    def get_location(json):
        s3 = boto3.client('s3')
        bucket_name = json["S3"]["Name"]
        filename_r = json["S3"]["FileName"]
        s3.download_file(bucket_name, filename_r, LOCATION_FILENAME_W)
        return load_json(LOCATION_FILENAME_W)


    # DOWNLOAD LOCATIONS FROM S3
    location_info = get_location(details)
    logging.info("Downloaded %s from bucket %s" % (details["S3"]["FileName"], details["S3"]["Name"]))

    # CREATE SensorMap
    sensor_map = SensorMap(location_info)


    def subscribe_q_to_notifications(topic_arn, queue_arn):
        sns = boto3.client('sns')
        sns.subscribe(
            TopicArn=topic_arn,
            Protocol='sqs',
            Endpoint=queue_arn
        )


    # SUBSCRIBE THE QUEUE TO RECEIVE SNS NOTIFICATIONS
    subscribe_q_to_notifications(topic_arn, queue_arn)
    logging.info('Subscribed queue to topic')


    def extract_message(response):
        try:
            receipt_handle = response['Messages'][0]['ReceiptHandle']
            body_str = response['Messages'][0]['Body']
            message_str = json.loads(body_str)['Message']
            message = json.loads(message_str)
            return message, receipt_handle
        except KeyError:
            logging.info('no new messages on sqs')
            raise KeyError


    def get_timestamp(timestamp_ms_str):
        timestamp_s = int(timestamp_ms_str) / 1000
        return datetime.utcfromtimestamp(timestamp_s).strftime("%Y-%m-%d %H:%M")


    # RECEIVE AND PROCESS THE MESSAGE FROM THE QUEUE
    # done = False
    event_id_collection = []
    end_time = datetime.now() + timedelta(0, DATA_COLLECTING_TIME_SEC)


    def thread_function(queue_url, event_id_collection, sensor_map, end_time):
        # STOP RECEIVING MESSAGES WHEN DATA_COLLECTING_TIME HAS BEEN REACHED
        while datetime.now() < end_time:
            response = sqs.receive_message(QueueUrl=queue_url, WaitTimeSeconds=1)

            # READ THE MESSAGE (EXTRACT_MESSAGE)
            message, receipt_handle = extract_message(response)
            location_id = message['locationId']
            event_id = message['eventId']
            value = message['value']
            timestamp = get_timestamp(message['timestamp'])

            def is_valid_id(location_id, location_info):
                london_id_list = []
                for location in location_info:
                    london_id_list.append(location['id'])
                return location_id in london_id_list

            def remove_old_ids(event_id_collection):
                # to keep things efficient keep event_id_collection small
                if len(event_id_collection) > MAX_SIZED_STORED:
                    event_id_collection.pop(0)


            # PROCESS THE MESSAGE
            if is_valid_id(location_id, location_info):
                if event_id not in event_id_collection:
                    event_id_collection.append(event_id)
                    remove_old_ids(event_id_collection)
                    # add data to the map
                    sensor_map.the_map[location_id].setdefault(timestamp, []).append(value)
                    logging.info("New data added to map")

                # DELETE MESSAGE
                boto3.resource('sqs').Message(queue_url, receipt_handle).delete()


    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        executor.submit(thread_function,
                        queue_url=queue_url,
                        event_id_collection=event_id_collection,
                        sensor_map=sensor_map,
                        end_time=end_time)


    # DELETE QUEUE
    sqs.delete_queue(QueueUrl=queue_url)
    logging.info("queue deleted")

    # OUTPUT MAP AS CSV FOR OFF APP PROCESSING
    with open('sensor_data.csv', 'w', newline='\n') as f:
        writer = csv.writer(f)
        for id, time_dict in sensor_map.the_map.items():
            for time_key, value_list in time_dict.items():
                avg = mean(value_list)
                row = [id, time_key, avg]
                writer.writerow(row)
