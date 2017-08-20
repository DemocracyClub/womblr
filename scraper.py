import csv
import datetime
import os
import random
import requests
import time
from dateutil.relativedelta import relativedelta
from polling_bot.brain import SlackClient
from sqlalchemy.exc import OperationalError

# hack to override sqlite database filename
# see: https://help.morph.io/t/using-python-3-with-morph-scraperwiki-fork/148
os.environ['SCRAPERWIKI_DATABASE_NAME'] = 'sqlite:///data.sqlite'
import scraperwiki


UPDATE_FREQUENCY = relativedelta(weeks=1)
ELECTIONS_IN_SCOPE = relativedelta(months=1)
MAX_OUTPUT_LINES = 30
SLACK_WEBHOOK_URL = os.environ['MORPH_SLACK_WEBHOOK_URL']
NOW = datetime.datetime.now()


try:
    latest = scraperwiki.sql.select("MAX(timestamp) AS ts FROM 'data';")
    last_run = datetime.datetime.strptime(latest[0]['ts'], '%Y-%m-%d %H:%M:%S.%f')
    if last_run + UPDATE_FREQUENCY > NOW:
        print('Nothing to do today..')
        quit()
except OperationalError:
    # The first time we run the scraper it will throw
    # because the table doesn't exist yet
    pass


def post_slack_message(message):
    slack = SlackClient(SLACK_WEBHOOK_URL)
    slack.post_message(message)

def call_ee(url):
    res = requests.get(url)
    res.raise_for_status()
    return res.json()

def call_ynr(url):
    res = requests.get(url)
    res.raise_for_status()
    decoded_content = res.content.decode('utf-8')
    cr = csv.reader(decoded_content.splitlines(), delimiter=',')
    return list(cr)

def get_emoji():
    return random.choice([
        ':satellite_antenna:',
        ':rotating_light:',
        ':ballot_box_with_ballot:',
        ':mega:',
        ':alarm_clock:',
        ':phone:',
    ])

def get_title():
    return random.choice([
        'Elections happening in the next month',
        'Election update!',
        'Elections coming up this month',
    ])

# get a list of upcoming elections
ee_url = "https://elections.democracyclub.org.uk/api/elections.json?future=1&limit=100"
elections = []
while ee_url:
    print(ee_url)
    ee_data = call_ee(ee_url)

    if ee_data['results']:
        for result in ee_data['results']:
            election_datetime = datetime.datetime.strptime(result['poll_open_date'], '%Y-%m-%d')
            threshold_date = NOW + ELECTIONS_IN_SCOPE
            if result['group_type'] == "organisation" and election_datetime < threshold_date:
                # get details of any candidates we hold for this election
                election_id = result['election_id']
                ynr_url = "https://candidates.democracyclub.org.uk/media/candidates-%s.csv" % (election_id)
                print(ynr_url)
                ynr_data = call_ynr(ynr_url)
                elections.append({
                    'timestamp': NOW,
                    'id': election_id,
                    'name': result['election_title'],
                    'known_candidates': len(ynr_data)-1,
                    'poll_open_date': result['poll_open_date'],
                    'url': "https://candidates.democracyclub.org.uk/election/%s/constituencies" % (election_id)
                })
                time.sleep(2)  # have a little snooze to avoid hammering the api

    time.sleep(1)  # have a little snooze to avoid hammering the api
    ee_url = ee_data['next']


# shove it all in the database
scraperwiki.sqlite.save(
    unique_keys=['timestamp', 'id'], data=elections, table_name='data')

print('=====')

# sort elections by date
elections = sorted(elections, key=lambda k: k['poll_open_date'])

# assemble slack mesages
slack_messages = [get_emoji() + ' *' + get_title() + '* ' + get_emoji()]
for election in elections:
    message = "%s: <%s|%s>. known candidates: %s" % (
        election['poll_open_date'],
        election['url'],
        election['name'],
        election['known_candidates'])
    if election['known_candidates'] == 0:
        message += " :womble: required"
    slack_messages.append(message)

if len(slack_messages) > MAX_OUTPUT_LINES:
    slack_message = "\n".join(slack_messages[:MAX_OUTPUT_LINES])
    slack_message += "\n" + "...for more details see <https://morph.io/DemocracyClub/womblr>"
else:
    slack_message = "\n".join(slack_messages)


print(slack_message)
# post it
post_slack_message(slack_message)
