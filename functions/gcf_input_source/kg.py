


import os

from google.cloud import pubsub_v1


geocode_request_topicname = os.environ.get('GEOCODE_REQUEST_TOPICNAME')
kg_request_topicname = os.environ.get('KG_REQUEST_TOPICNAME')
# int(os.environ.get('TIMEOUT'))

# An array of Future objects
# Every call to publish() returns an instance of Future
geocode_futures = []
kg_futures = []

pub_client = pubsub_v1.PublisherClient()

def get_env():
    print(os.environ)
    if 'GCP_PROJECT' in os.environ:
        return os.environ['GCP_PROJECT']
    import google.auth
    _, project_id = google.auth.default()
    print(project_id)
    return project_id

project_id = get_env()

def sendKGRequest(message_data):
    print("sendKGRequest")

    kg_topic_path = pub_client.topic_path(
        project_id, kg_request_topicname)
    print("kg_topic_path")
    print(kg_topic_path)

    kg_future = pub_client.publish(
        kg_topic_path, data=message_data)
    kg_futures.append(kg_future)

def sendGeoCodeRequest(message_data):
    print("sendGeoCodeRequest")
    print(message_data)
    geocode_topic_path = pub_client.topic_path(
        project_id, geocode_request_topicname)
    
    print("geocode_topic_path")
    print(geocode_topic_path)

    geocode_future = pub_client.publish(
        geocode_topic_path, data=message_data)
    geocode_futures.append(geocode_future)
