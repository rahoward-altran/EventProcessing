import boto3
import json
import logging
from SensorMap import SensorMap

LOCATION_FILENAME_W = 'locations.json'

def load_json(filename):
    with open(filename) as json_file:
        return json.load(json_file)

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

def is_valid_id(location_id, location_info):
    # london_id_list = [item['id'] for item in location_info]
    london_id_list = []
    for location in location_info:
        london_id_list.append(location['id'])
    return location_id in london_id_list

# SET UP LOGGING:
logging.basicConfig(filename='Log.log', filemode='w', level=logging.INFO)
logging.info("Program has started and the log is open")

# READ EP_info.json
details = load_json('EP_info.json')
logging.info("EP_info.json has been read")


def create_queue(sqs_cli, topic_arn):
    queue_url = sqs_cli.create_queue(
        QueueName='event_notification_queue'
    )['QueueUrl']
    queue_arn = sqs_cli.get_queue_attributes(
        QueueUrl=queue_url,
        AttributeNames=['QueueArn']
    )['Attributes']['QueueArn']
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
    response = sns.subscribe(
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

# RECEIVE AND PROCESS THE MESSAGE FROM THE QUEUE
done = False
while not done:
    response = sqs.receive_message(
        QueueUrl=queue_url
    )
    try:
        message, receipt_handle = extract_message(response)
        location_id = message['locationId']
        # event_id = message['eventId']
        value = message['value']
        timestamp = message['timestamp']
        data = sensor_map.the_map[location_id].setdefault(timestamp, []).append(value)
        print(data)

        #delete message
        # receipt handle = message
        sqs_resource = boto3.resource('sqs')
        delete_me = sqs_resource.Message(queue_url, receipt_handle)
        delete_me.delete()
    except KeyError:
        done = True
    except KeyboardInterrupt:
        done = True

    # process_message(message)

# DELETE QUEUE
response = sqs.delete_queue(
    QueueUrl=queue_url
)
