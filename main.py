"""Standard library"""
import os

"""Third party modules"""
from concurrent.futures import TimeoutError
from google.cloud import pubsub_v1
import pymsteams

"""
The Cloud Function will be triggered by the Pub/Sub Topic when it receives messages from
the Falco Sidekick running in the Kubernetes clusters. The Cloud Function will then process
the messages receieved and send an alert notification to the Microsoft Teams channel when
any critical vulnerabilities are found.
"""


def alert_notification(message: str) -> None:
    """Send alert notification to Microsoft Teams"""

    microsoft_webhook_url = os.environ.get('MICROSOFT_WEBHOOK_URL', 'Specified environment variable is not set.')
    ms_teams_message = pymsteams.connectorcard(microsoft_webhook_url)
    ms_teams_message.text(message)
    ms_teams_message.send()

def callback(message: pubsub_v1.subscriber.message.Message) -> None:
    """Process the messages received from the Pub/Sub subscription"""

    print(f"Received {message.data}.")
    if message.attributes:
        print("Attributes:")
        for key in message.attributes:
            value = message.attributes.get(key)
            print(f"{key}: {value}")
    message.ack()

    # TODO: call alert_notification function with messages that contain critical findings

def pull_messages(project_id: str, subscription_id: str, timeout: float) -> None:
    """Pull the available messages from Pub/Sub subscription"""

    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(project_id, subscription_id)

    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
    print(f"Listening for messages on {subscription_path}..\n")

    with subscriber:
        try:
            streaming_pull_future.result(timeout=timeout)
        except TimeoutError:
            streaming_pull_future.cancel()
            streaming_pull_future.result()

if __name__ == "__main__":

    project_id = os.environ.get('PROJECT_ID', 'Specified environment variable is not set.')
    subscription_id = os.environ.get('SUBSCRIPTION_ID', 'Specified environment variable is not set.')
    timeout = 5.0
    
    pull_messages(project_id=project_id, subscription_id=subscription_id, timeout=timeout)
