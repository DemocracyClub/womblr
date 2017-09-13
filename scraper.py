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
try:
    SLACK_WEBHOOK_URL = os.environ['MORPH_SLACK_WEBHOOK_URL']
except KeyError:
    SLACK_WEBHOOK_URL = None
NOW = datetime.datetime.now()


def post_slack_message(message):
    slack = SlackClient(SLACK_WEBHOOK_URL)
    slack.post_message(message)

def call_json_api(url):
    res = requests.get(url)
    res.raise_for_status()
    return res.json()

def call_csv_api(url):
    res = requests.get(url)
    res.raise_for_status()
    decoded_content = res.content.decode('utf-8')
    cr = csv.DictReader(decoded_content.splitlines(), delimiter=',')
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

def format_date(d):
    return datetime.datetime.strptime(d, "%Y-%m-%d").strftime("%d/%m/%Y")

def get_slack_message(elections):
    # sort elections by date
    elections = sorted(elections, key=lambda k: k['poll_open_date'])

    # assemble slack mesages
    slack_messages = [get_emoji() + ' *' + get_title() + '* ' + get_emoji()]
    for election in elections:
        message = "%s: <%s|%s>. known candidates: %s" % (
            format_date(election['poll_open_date']),
            election['url'],
            election['name'],
            election['known_candidates'])
        if election['known_candidates'] == 0:
            message += " :womble: required"
        if 'locked' in election and election['locked']:
            message += " :lock:"
        slack_messages.append(message)

    if len(slack_messages) > MAX_OUTPUT_LINES:
        slack_message = "\n".join(slack_messages[:MAX_OUTPUT_LINES])
        slack_message += "\n" + "...for more details see <https://morph.io/DemocracyClub/womblr>"
    else:
        slack_message = "\n".join(slack_messages)

    return slack_message

def get_posts(candidates):
    posts = {}
    for candidate in candidates:
        if candidate['post_id'] not in posts:
            posts[candidate['post_id']] = {
                'known_candidates': 1,
                'locked': False
            }
            ynr_url = 'https://candidates.democracyclub.org.uk/api/v0.9/posts/' + candidate['post_id']
            print(ynr_url)
            post = call_json_api(ynr_url)
            posts[candidate['post_id']]['label'] = post['label']
            for election in post['elections']:
                if election['id'] == candidate['election']:
                    posts[candidate['post_id']]['locked'] = election['candidates_locked']
        else:
            posts[candidate['post_id']]['known_candidates'] += 1
    return posts

def get_elections():
    elections = []
    # get a list of upcoming elections
    ee_url = "https://elections.democracyclub.org.uk/api/elections.json?future=1&limit=100"
    while ee_url:
        print(ee_url)
        ee_data = call_json_api(ee_url)

        if ee_data['results']:
            for result in ee_data['results']:
                election_datetime = datetime.datetime.strptime(result['poll_open_date'], '%Y-%m-%d')
                threshold_date = NOW + ELECTIONS_IN_SCOPE
                if result['group_type'] == "organisation" and election_datetime < threshold_date:
                    # get details of any candidates we hold for this election
                    election_id = result['election_id']
                    ynr_url = "https://candidates.democracyclub.org.uk/media/candidates-%s.csv" % (election_id)
                    print(ynr_url)

                    try:
                        ynr_data = call_csv_api(ynr_url)
                        total_candidates = len(ynr_data)
                        posts = get_posts(ynr_data)
                    except requests.exceptions.HTTPError:
                        total_candidates = 0

                    if total_candidates == 0 or not posts:
                        elections.append({
                            'timestamp': NOW,
                            'id': election_id,
                            'name': result['election_title'],
                            'known_candidates': total_candidates,
                            'poll_open_date': result['poll_open_date'],
                            'url': "https://candidates.democracyclub.org.uk/election/%s/constituencies" % (election_id),
                            'post_id': None,
                            'locked': False,
                        })
                    else:
                        for post in posts:
                            elections.append({
                                'timestamp': NOW,
                                'id': election_id,
                                'name': "%s - %s" % (result['election_title'], posts[post]['label']),
                                'known_candidates': posts[post]['known_candidates'],
                                'poll_open_date': result['poll_open_date'],
                                'url': "https://candidates.democracyclub.org.uk/election/%s/post/%s" % (election_id, post),
                                'post_id': post,
                                'locked': posts[post]['locked'],
                            })
                    time.sleep(2)  # have a little snooze to avoid hammering the api

        time.sleep(1)  # have a little snooze to avoid hammering the api
        ee_url = ee_data['next']

    return elections

def scrape():
    elections = get_elections()
    scraperwiki.sqlite.save(
        unique_keys=['timestamp', 'id', 'post_id'], data=elections, table_name='data')
    print('=====')
    slack_message = get_slack_message(elections)
    print(slack_message)
    if SLACK_WEBHOOK_URL:
        post_slack_message(slack_message)


try:
    latest = scraperwiki.sql.select("MAX(timestamp) AS ts FROM 'data';")
    last_run = datetime.datetime.strptime(latest[0]['ts'], '%Y-%m-%d %H:%M:%S.%f')
    if last_run + UPDATE_FREQUENCY > NOW:
        print("Nothing to do today, but here's the results from the last run..")
        elections = scraperwiki.sql.select(
            "* FROM 'data' WHERE timestamp=?;", [latest[0]['ts']])
        print('=====')
        slack_message = get_slack_message(elections)
        print(slack_message)
    else:
        scrape()
except OperationalError:
    # The first time we run the scraper it will throw
    # because the table doesn't exist yet
    scrape()
