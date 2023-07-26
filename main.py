import pandas as pd
import json
import boto3
from botocore import exceptions
from string import Template
import logging
import sys
import io
import requests
from datetime import datetime, timedelta

session = boto3.Session()
s3 = session.client('s3')
ssm = session.client('ssm')
sns = session.client('sns')

BUCKET = '2w-nr-muting-rules-automation'
TOPIC_ARN = 'arn:aws:sns:us-east-1:187940856853:2w-nr-muting-rules-automation-topic'


def initialize_logger():
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)

    return logger


def get_stored_rule_data(logger):
    logger.info('Fetching muting rule info...')

    key = 'Muting Rules.xlsx'

    muting_rules_file = s3.get_object(Bucket=BUCKET, Key=key)
    muting_rules_data = muting_rules_file['Body'].read()

    columns = ['Client', 'Environment', 'Muting Rule ID', 'NR Account #']
    muting_df = pd.read_excel(io.BytesIO(muting_rules_data), usecols=columns)

    if not muting_df.empty:
        logger.info('   Muting rule IDs loaded successfully.')
        return muting_df
    else:
        logger.warning('   No muting rule data found.')
        sys.exit(1)


def get_api_key(api, logger):
    param_dict = {
        'Monday': 'ae-muting-automation-monday-key',
        'New Relic': 'ae-muting-automation-new-relic-key'
    }
    try:
        response = ssm.get_parameter(Name=param_dict[api], WithDecryption=True)
        key = response['Parameter']['Value']
        logger.info(f'   {api} key retrieved successfully.')
        return key
    except exceptions.ClientError as e:
        logger.warning(f'\nAPI key not retrieved from Parameter Store:\n{e}')
        sys.exit(1)


def get_muting_rule_info(client, envir, df, logger):
    logger.info('   Extracting muting rule ID and New Relic account number...')

    # Special handling for Lenovo patching event names
    if client == 'Lenovo':
        if 'Linux' in envir:
            envir = 'Weekly Linux'
        elif 'Windows' in envir:
            envir = 'Weekly Windows'

    try:
        rule_df = df[(df['Client'] == client) & (df['Environment'] == envir)]
        rule_ids = [int(value) for value in rule_df['Muting Rule ID']]
        nr_account = df.loc[(df['Client'] == client) & (df['Environment'] == envir), 'NR Account #'].iloc[0]
        logger.info(f'      Muting Rule ID(s): {rule_ids} in NR Account: {int(nr_account)}')
        return rule_ids, int(nr_account)
    except Exception as e:
        error_type = e.__class__.__name__
        if error_type == 'IndexError':
            logger.warning(f'      {client} does not have muting rule information.')
        elif error_type == 'ValueError':
            logger.warning(f'      {client} {envir} does not have a muting rule in place.')
        else:
            logger.warning(f'      There was an error extracting muting rule information:\n'
                           f'      {e.__class__.__name__}: {e}')
        return None, None


def get_patching_event(event, logger):
    logger.info('Fetching patching event from pulseId...')

    event = event['event']
    board_id = event['boardId']
    pulse_id = event['pulseId']

    if board_id == 3766513763:
        message = 'This event was triggered from the Tooling Team board; no action taken.'
        logger.info(message)
        return [1, message]
    else:
        # Monday API call data
        api_token = get_api_key('Monday', logger)
        endpoint = 'https://api.monday.com/v2'
        headers = {
            'Authorization': api_token,
            'Content-Type': 'application/json'
        }
        # Monday board GraphQL query to filter for specific columns
        gql_query_template = Template("""
        {
          boards (ids: $board_id) {
            items (ids: $pulse_id) {
              name
              id
              column_values (ids: [text, status]) {
                title
                text
              }
            }
          }
        }
        """)

        gql_query_fmtd = gql_query_template.substitute({'board_id': board_id, 'pulse_id': pulse_id})

        # Call the Monday API and transform the response into JSON format
        response = requests.get(endpoint, headers=headers, json={'query': gql_query_fmtd}).json()
        logger.debug(f'Monday API response:\n{response}')

        if 'errors' in response.keys():
            logger.warning(f'There was an error calling the Monday API:\n{response}')
            sys.exit(1)
        else:
            monday_item = response['data']['boards'][0]['items']
            logger.info(f'Patching event found: {monday_item}')
            return monday_item


def is_before_schedule(muting_rule_id, start, logger):
    is_early = False

    # Special handling for Neighborly events
    if muting_rule_id in [38495968, 38496605]:
        now = datetime.now() + timedelta(hours=1.0)
        now_converted = datetime.strftime(now + timedelta(hours=1.0), '%Y-%m-%dT%H:%M:%S')
        start_dt = datetime.strptime(start, '%Y-%m-%dT%H:%M:%S-06:00') + timedelta(hours=1.0)
    else:
        now = datetime.now()
        now_converted = datetime.strftime(now, '%Y-%m-%dT%H:%M:%S')
        start_dt = datetime.strptime(start, '%Y-%m-%dT%H:%M:%S-06:00')

    if now < start_dt:
        logger.info('Event start is earlier than scheduled start. Enabling muting...')
        is_early = True
    else:
        logger.info('Event start is earlier than scheduled start.')

    return is_early, now_converted


def change_muting_rule_status(monday_item, muting_df, logger):
    logger.info('Processing patching events...')
    messages = []

    try:
        # NR API details
        nr_api_key = get_api_key('New Relic', logger)
        nr_endpoint = 'https://api.newrelic.com/graphql'
        nr_headers = {
            'Content-Type': 'application/json',
            'API-Key': nr_api_key,
        }
        nr_gql_query_template = Template("""
                {
                  actor {
                    account(id: $account_id) {
                      alerts {
                        mutingRule(id: $rule_id) {
                          id
                          enabled
                          schedule {
                            startTime
                          }
                        }
                      }
                    }
                  }
                }
            """)
        nr_gql_enable_template = Template("""
            mutation {
              alertsMutingRuleUpdate(
                accountId: $account_id
                id: $rule_id
                rule: {schedule: {startTime: "$start_time"}, enabled: $enabled}
              )
            }
            """)
        nr_gql_disable_template = Template("""
            mutation {
                alertsMutingRuleUpdate(accountId: $account_id, id: $rule_id, rule: 
                  {enabled: $enabled}) {
                  id
              }
            }
            """)

        for i in range(len(monday_item)):
            client_name = monday_item[i]['name']
            # Used for testing
            if client_name == 'Test Project':
                environment = 'for Tooling'
                event_status = monday_item[i]['column_values'][0]['text']
            else:
                environment = monday_item[i]['column_values'][0]['text']
                event_status = monday_item[i]['column_values'][1]['text']

            clients_without_muting = ['2W Infra', 'Gas Station TV', 'Michael Kors', 'NAIC', 'Symetra', 'TitleMax']

            if client_name in clients_without_muting:
                message = f'{client_name} does not have muting rules in place; skipping event.'
                logger.info(f'   {message}')
                messages.append(message)
                continue
            elif event_status not in ['Event Complete', 'All Compliant', 'Done']:
                message = f'Patching event for {client_name} {environment} is not complete and has a status of ' \
                          f'{event_status}; skipping event.'
                logger.info(f'   {message}')
                messages.append(message)
                continue
            else:
                logger.info(f'\nEvent {i + 1}: {event_status} --> {client_name} {environment}.')

                # Muting rule ID and account corresponding to patching event data
                muting_rule_ids, nr_account_num = get_muting_rule_info(client_name, environment, muting_df, logger)

                # Used only for testing
                if client_name == 'Test Project':
                    muting_rule_ids = [38434772]
                    nr_account_num = 3720977
                if not muting_rule_ids:
                    continue

                logger.info(f'Checking muting rule status for this event...')

                for muting_rule_id in muting_rule_ids:
                    # Query rule to check if it is enabled or disabled
                    nr_gql_query_fmtd = nr_gql_query_template.substitute({'account_id': nr_account_num,
                                                                          'rule_id': muting_rule_id})
                    nr_response = requests.post(nr_endpoint,
                                                headers=nr_headers,
                                                json={'query': nr_gql_query_fmtd}).json()
                    logger.debug(f'New Relic API response:\n{nr_response}')

                    try:
                        # If the 'errors' key exists in the API response, log the error
                        message = f'There was an error querying muting role {muting_rule_id} for {client_name} ' \
                                  f'{environment}: {nr_response["errors"][0]["message"]}'
                        logger.warning(f'{message}')
                        messages.append(message)
                    except KeyError:
                        is_enabled = nr_response['data']['actor']['account']['alerts']['mutingRule']['enabled']
                        start = nr_response['data']['actor']['account']['alerts']['mutingRule']['schedule']['startTime']

                        is_early, now_converted = is_before_schedule(muting_rule_id, start, logger)

                        if event_status == 'Event In Progress' and is_early:
                            # mutate muting rule for earlier start
                            nr_gql_enable_fmtd = nr_gql_enable_template.substitute(
                                {'account_id': nr_account_num,
                                 'rule_id': muting_rule_id,
                                 'start_time': now_converted,
                                 'enabled': 'true'})
                            nr_response = requests.post(nr_endpoint,
                                                        headers=nr_headers,
                                                        json={'query': nr_gql_enable_fmtd}).json()
                            logger.debug(f'New Relic API response:\n{nr_response}')

                            if nr_response['data']['alertsMutingRuleUpdate']['id'] == str(muting_rule_id):
                                message = f'Muting rule {muting_rule_id} for {client_name} {environment} was ' \
                                          f'successfully enabled for an early event start.'
                                logger.info(f'{message}')
                                messages.append(message)
                            else:
                                message = f'There was an error enabling muting rule {muting_rule_id} for ' \
                                          f'{client_name} {environment}: {nr_response}'
                                logger.warning(f'{message}')
                                messages.append(message)
                                continue
                            continue
                        elif event_status == 'Event Complete' and not is_enabled:
                            message = f'Muting rule {muting_rule_id} for {client_name} {environment} is already ' \
                                      f'disabled; no action taken.'
                            logger.info(f'{message}')
                            messages.append(message)
                            continue
                        else:
                            nr_gql_disable_fmtd = nr_gql_disable_template.substitute(
                                {'account_id': nr_account_num,
                                 'rule_id': muting_rule_id,
                                 'enabled': 'false'})
                            nr_response = requests.post(nr_endpoint,
                                                        headers=nr_headers,
                                                        json={'query': nr_gql_disable_fmtd}).json()
                            logger.debug(f'New Relic API response:\n{nr_response}')

                            if nr_response['data']['alertsMutingRuleUpdate']['id'] == str(muting_rule_id):
                                message = f'Muting rule {muting_rule_id} for {client_name} {environment} was ' \
                                          f'successfully disabled.'
                                logger.info(f'{message}')
                                messages.append(message)
                            else:
                                message = f'There was an error disabling muting rule {muting_rule_id} for ' \
                                          f'{client_name} {environment}: {nr_response}'
                                logger.warning(f'{message}')
                                messages.append(message)
                                continue
        return 0, messages
    except Exception as e:
        message = f'There was a general error: {e}'
        logger.warning(message)
        return 1, [message]


def handler(event, context):
    logger = initialize_logger()

    body = event.get('body', '{}')
    event_obj = json.loads(body)
    logger.info(event_obj)

    try:
        # Monday webhook challenge happens one time per webhook integration setup
        challenge = event_obj['challenge']

        return {
            "isBase64Encoded": True,
            "statusCode": 200,
            "headers": {},
            "body": json.dumps({"challenge": challenge})
        }

    except KeyError:
        # Process all subsequent webhook event payloads
        muting_df = get_stored_rule_data(logger)
        monday_item = get_patching_event(event_obj, logger)
        if len(monday_item) > 1:
            subject = 'Event-based muting rule mutation test'
            sns_message = monday_item[1]
        else:
            process_code, messages = change_muting_rule_status(monday_item, muting_df, logger)

            if process_code < 1:
                logger.info('')
                subject = 'Event-based muting rule mutation success'
            else:
                subject = 'Event-based muting rule mutation error'

            sns_message = ''
            for message in messages:
                sns_message += f'{message}\n'

        # Send an SNS notification upon code completion
        response = sns.publish(TopicArn=TOPIC_ARN, Subject=subject, Message=sns_message)
        logger.info(response)

        return {
            'statusCode': 200,
            'body': 'Success!'
        }
