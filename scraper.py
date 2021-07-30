import datetime
import os
import random
import requests
import time
from dateutil.relativedelta import relativedelta
from polling_bot.brain import SlackClient
from sopn_publish_date import StatementPublishDate, Country
from sopn_publish_date.election_ids import (
    InvalidElectionIdError,
    NoSuchElectionTypeError,
)

# hack to override sqlite database filename
# see: https://help.morph.io/t/using-python-3-with-morph-scraperwiki-fork/148
os.environ['SCRAPERWIKI_DATABASE_NAME'] = 'sqlite:///data.sqlite'
import scraperwiki


UPDATE_FREQUENCY = relativedelta(weeks=1)
ELECTIONS_IN_SCOPE = relativedelta(months=1)
MAX_OUTPUT_LINES = 30
try:
    SLACK_WEBHOOK_URL = os.environ['MORPH_ORINOCO_SLACK_WEBHOOK_URL']
except KeyError:
    SLACK_WEBHOOK_URL = None
NOW = datetime.datetime.now()
SOPN_PUBLISH_DATE = StatementPublishDate()


def init():
    table_info = scraperwiki.sql.execute("PRAGMA table_info(data);")
    if table_info.get('data'):
        if 'post_id' in [row[1] for row in table_info['data']]:
            scraperwiki.sql.execute("DROP TABLE data;")

    scraperwiki.sql.execute("""
        CREATE TABLE IF NOT EXISTS data (
            ballot_id TEXT,
            timestamp DATETIME,
            url TEXT,
            poll_open_date TEXT,
            locked BOOLEAN,
            name TEXT,
            known_candidates BIGINT,
            sopn_published TEXT,
            has_sopn BOOLEAN,
            CHECK (locked IN (0, 1) AND has_sopn IN (0, 1))
        );""")

    scraperwiki.sql.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS data_timestamp_id_postid_unique
        ON data (timestamp, ballot_id);""")

def post_slack_message(message):
    slack = SlackClient(SLACK_WEBHOOK_URL)
    slack.post_message(message)

def call_json_api(url):
    res = requests.get(url)
    res.raise_for_status()
    return res.json()

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
        'By-Elections happening in the next month',
        'By-Election update!',
        'By-Elections coming up this month',
    ])

def format_date(d):
    return datetime.datetime.strptime(d, "%Y-%m-%d").strftime("%d/%m/%Y")

def get_sopn_message(ballot):
    if ballot['sopn_published'] is None:
        return None

    if ballot['has_sopn']:
        return " (SoPN uploaded)"
    elif ballot['sopn_published'] < str(datetime.date.today()):
        return " (SoPN should be published)"
    else:
        return " (SoPN due %s)" % format_date(ballot['sopn_published'])

def get_slack_message(elections):
    # sort elections by date
    elections = sorted(elections, key=lambda k: k['poll_open_date'])

    # assemble slack mesages
    slack_messages = [get_emoji() + ' *' + get_title() + '* ' + get_emoji()]
    for election in elections:
        sopn_message = get_sopn_message(election)

        message = "%s: <%s|%s>. known candidates: %s" % (
            format_date(election['poll_open_date']),
            election['url'],
            election['name'],
            election['known_candidates'])
        if election['known_candidates'] == 0:
            message += " :womble: required"
        if 'locked' in election and election['locked']:
            message += " :lock:"
        elif sopn_message is not None:
            message += sopn_message

        slack_messages.append(message)

    if len(slack_messages) > MAX_OUTPUT_LINES:
        slack_message = "\n".join(slack_messages[:MAX_OUTPUT_LINES])
        slack_message += "\n" + "...for more details see <https://morph.io/DemocracyClub/womblr>"
    else:
        slack_message = "\n".join(slack_messages)

    return slack_message


def get_sopn_date(result):
    election_id = result['election_id']

    territory = result['organisation']['territory_code']

    country = {
        "ENG": Country.ENGLAND,
        "WLS": Country.WALES,
        "SCT": Country.SCOTLAND,
        "NIR": Country.NORTHERN_IRELAND,
    }.get(territory, None)

    try:
        return SOPN_PUBLISH_DATE.for_id(election_id, country=country)
    except (InvalidElectionIdError, NoSuchElectionTypeError):
        return None


def get_ballots():
    ballots = []
    # get a list of upcoming elections
    ee_url = "https://elections.democracyclub.org.uk/api/elections.json?future=1&limit=100"
    while ee_url:
        print(ee_url)
        ee_data = call_json_api(ee_url)

        if not ee_data['results']:
            continue

        ee_ballots = [
            election for election in ee_data['results']
            if election ['identifier_type'] == 'ballot'
        ]
        for ee_ballot in ee_ballots:
            election_datetime = datetime.datetime.strptime(ee_ballot['poll_open_date'], '%Y-%m-%d')
            threshold_date = NOW + ELECTIONS_IN_SCOPE
            if election_datetime >= threshold_date:
                continue

            ballot_id = ee_ballot['election_id']
            sopn_date = get_sopn_date(ee_ballot)

            ynr_ballot_url = 'https://candidates.democracyclub.org.uk/api/next/ballots/{}/'.format(ballot_id)
            print(ynr_ballot_url)

            try:
                ynr_ballot = call_json_api(ynr_ballot_url)
            except requests.exceptions.HTTPError:
                ballots.append({
                    'timestamp': NOW,
                    'ballot_id': ballot_id,
                    'name': ee_ballot['election_title'],
                    'known_candidates': 0,
                    'poll_open_date': ee_ballot['poll_open_date'],
                    'url': "https://candidates.democracyclub.org.uk/elections/{}/".format(ballot_id),
                    'locked': False,
                    'sopn_published': str(sopn_date) if sopn_date is not None else None,
                    'has_sopn': False,
                })
                continue

            ballots.append({
                'timestamp': NOW,
                'ballot_id': ballot_id,
                'name': ee_ballot['election_title'],
                'known_candidates': len(ynr_ballot['candidacies']),
                'poll_open_date': ee_ballot['poll_open_date'],
                'url': "https://candidates.democracyclub.org.uk/elections/{}/".format(ballot_id),
                'locked': ynr_ballot['candidates_locked'],
                'sopn_published': str(sopn_date) if sopn_date is not None else None,
                'has_sopn': bool(ynr_ballot['sopn']),
            })

            time.sleep(2)  # have a little snooze to avoid hammering the api

        time.sleep(1)  # have a little snooze to avoid hammering the api
        ee_url = ee_data['next']

    return ballots

def scrape():
    ballots = get_ballots()
    scraperwiki.sqlite.save(
        unique_keys=['timestamp', 'ballot_id'], data=ballots, table_name='data')
    print('=====')
    slack_message = get_slack_message(ballots)
    print(slack_message)
    if SLACK_WEBHOOK_URL:
        post_slack_message(slack_message)


init() # make sure our tables exist
latest = scraperwiki.sql.select("MAX(timestamp) AS ts FROM 'data';")

if latest[0]['ts'] is None:
    # this is the first time we've ever run
    scrape()
    raise SystemExit(0)

last_run = datetime.datetime.strptime(latest[0]['ts'], '%Y-%m-%d %H:%M:%S.%f')
if last_run + UPDATE_FREQUENCY > NOW:
    print("Nothing to do today, but here's the results from the last run..")
    ballots = scraperwiki.sql.select(
        "* FROM 'data' WHERE timestamp=?;", [latest[0]['ts']])
    print('=====')
    slack_message = get_slack_message(ballots)
    print(slack_message)
else:
    scrape()
