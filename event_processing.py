import boto3
import json
import csv
import logging
from multiprocessing import Manager, Process
from statistics import mean
from datetime import datetime, timedelta
from SensorMap import SensorMap

LOCATION_FILENAME_W = 'locations_part3.json'
EP_INFO_FILENAME = 'EP_info_part3.json'
DATA_COLLECTING_TIME_SEC = 3600
BATCH_NUMBER = 10


def load_json(filename):
    with open(filename) as json_file:
        return json.load(json_file)


def extract_messages(messages):
    message_list = []
    delete_list = []

    for message in messages:
        try:
            delete_list.append({
                'Id': message['MessageId'],
                'ReceiptHandle': message['ReceiptHandle']
            })
            body_str = message['Body']
            message_str = json.loads(body_str)['Message']
            message_list.append(json.loads(message_str))
        except KeyError:
            logging.info('no new messages on sqs')
            raise KeyError
    return message_list, delete_list


def get_timestamp(timestamp_ms_str):
    timestamp_s = int(timestamp_ms_str) / 1000
    return datetime.utcfromtimestamp(timestamp_s).strftime("%Y-%m-%d %H:%M")


def thread_function(queue_url, end_time, location_info, q):
    # STOP RECEIVING MESSAGES WHEN DATA_COLLECTING_TIME HAS BEEN REACHED
    while datetime.now() < end_time:
        response = boto3.client('sqs').receive_message(QueueUrl=queue_url,
                                                       WaitTimeSeconds=1,
                                                       MaxNumberOfMessages=10)
        # READ THE MESSAGE (EXTRACT_MESSAGE)
        try:
            messages = response['Messages']
            message_list, delete_list = extract_messages(messages)

            for message in message_list:
                location_id = message['locationId']
                # event_id = message['eventId']
                value = message['value']
                timestamp = get_timestamp(message['timestamp'])

                def is_valid_id(location_id, location_info):
                    london_id_list = []
                    for location in location_info:
                        london_id_list.append(location['id'])
                    return location_id in london_id_list

                # PROCESS THE MESSAGE
                if is_valid_id(location_id, location_info):
                    # Todo: handle event id duplicates
                    q.put({
                        'location_id': location_id,
                        'timestamp': timestamp,
                        'value': value
                    })
        except KeyError:
            # message not constructed as expected / no messages were received
            pass


        # DELETE MESSAGES
        boto3.client('sqs').delete_message_batch(
            QueueUrl=queue_url,
            Entries=delete_list
        )


if __name__ == "__main__":
    # SET UP LOGGING:
    logging.basicConfig(filename='Log.log', filemode='w', level=logging.DEBUG)
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


    # CREATE QUEUE
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


    # RECEIVE AND PROCESS THE MESSAGE FROM THE QUEUE USING MULTIPROCESSING
    # event_id_collection = multiprocessing.Array(c_char_p, MAX_SIZED_STORED, lock=False)
    q = Manager().Queue()
    end_time = datetime.now() + timedelta(0, DATA_COLLECTING_TIME_SEC)

    logging.info('Before creating process')
    p1 = Process(target=thread_function,
                 args=(queue_url, end_time, location_info, q)
                 )
    p2 = Process(target=thread_function,
                 args=(queue_url, end_time, location_info, q)
                 )

    p1.start()
    p2.start()
    logging.info("multiprocessing started")

    p1.join()
    p2.join()
    logging.info("multiprocessing joined")

    # DELETE QUEUE
    sqs.delete_queue(QueueUrl=queue_url)
    logging.info("queue deleted")


    def q_to_map(q, sensor_map):
        while not q.empty():
            data_point = q.get_nowait()
            # add data to the map
            sensor_map.the_map[data_point['location_id']].setdefault(data_point['timestamp'], []).append(data_point['value'])
            logging.info("New data added to map")


    # BUCKETS THE MULTIPROCESSING QUEUE DATA INTO {ID : {TIME : [VALUES]}}
    q_to_map(q, sensor_map)

    # OUTPUT MAP AS CSV FOR OFF APP PROCESSING
    with open('sensor_data.csv', 'w', newline='\n') as f:
        writer = csv.writer(f)
        for loc_id, time_dict in sensor_map.the_map.items():
            for time_key, value_list in time_dict.items():
                avg = mean(value_list)
                row = [loc_id, time_key, avg]
                writer.writerow(row)
    logging.info("Sensor data has been written as a csv")

