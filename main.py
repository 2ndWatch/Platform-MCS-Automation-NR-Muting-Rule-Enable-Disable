import json


def handler(event, context):
    body = event.get('event', '{}')
    event_obj = json.loads(body)
    # TODO: parse payload, get itemId
    # TODO: call Monday API to get client name and environment
    # TODO: find muting rule ID and NR account #
    # TODO: call NR API to disable relevant muting rule

    return {
        'statusCode': 200,
        'headers': {
            'Content-Type': 'application/json'
        }
    }
