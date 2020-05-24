import sys
import time
import warnings

import pandas as pd
from sqlalchemy import and_, create_engine
from sqlalchemy.exc import SAWarning
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session, sessionmaker

warnings.filterwarnings('ignore', r".*support Decimal objects natively",
                        SAWarning, r"^sqlalchemy\.sql\.sqltypes$")

TZ = 'America/Fortaleza'
NAP_UID = 'qpbgpcWOXOIRX0173470082348182010'
SLEEP_UID = 'nydndmixbpEU27753063060301756015'
BABY_BDAY = pd.Timestamp(2019, 12, 20).tz_localize('UTC').tz_convert(TZ)
MAX_NAP_INTERVAL = 20
MAX_SLEEP_INTERVAL = 60

engine = create_engine('sqlite:///BabyDaybook.db')
Session = sessionmaker()
Session.configure(bind=engine)
session = Session()

Base = automap_base()
Base.prepare(engine, reflect=True)
Baby = Base.classes.babies
Action = Base.classes.daily_actions
Groups = Base.classes.groups


diapers_change = session.query(Action).order_by(Action.start_millis).filter_by(
    type='diaper_change').all()
sleeps = session.query(Action).order_by(Action.start_millis).filter_by(
    type='sleeping').filter(Action.start_millis < Action.end_millis).all()


def _convert_date_to_millis(date=None):
    return int(date.timestamp() * 1000)


def _convert_millis_to_date(millis=None):
    return pd.to_datetime(int(float(millis)), unit='ms').tz_localize('UTC').tz_convert(TZ)


def _convert_millis_to_minutes(millis=None):
    return int(pd.Timedelta(float(millis), unit='milliseconds').total_seconds()/60)


def _format_start_end(action=None):
    start = _convert_millis_to_date(action.start_millis)
    end = _convert_millis_to_date(action.end_millis)
    return start, end


def _calc_interval(action=None):
    start, end = _format_start_end(action)
    return pd.Interval(start, end)


def _get_start_of_the_day(date):
    return date.normalize() + pd.Timedelta('5 hours')


def _get_end_of_the_day(date):
    return date.normalize() + pd.Timedelta('1 day') - pd.Timedelta('1 ms')


def _get_yesterday(date):
    return date.normalize() - pd.Timedelta('1 day')


def _get_now_in_millis():
    return int(pd.Timestamp.today().timestamp()*1000)


join = dict()
wrong_groups = dict()

for date in pd.date_range(start=BABY_BDAY, end=pd.Timestamp.today(tz=TZ), tz=TZ, normalize=True, closed='left'):
    try:
        start_of_today = _convert_date_to_millis(_get_start_of_the_day(date))
        end_of_today = _convert_date_to_millis(_get_end_of_the_day(date))
        start_of_yesterday = _convert_date_to_millis(
            _get_start_of_the_day(_get_yesterday(date)))
        end_of_yesterday = _convert_date_to_millis(
            _get_end_of_the_day(_get_yesterday(date)))

        diapers_change_of_today = list(filter(
            lambda diaper_change:
            diaper_change.start_millis >= start_of_today and
            diaper_change.start_millis <= end_of_today,
            diapers_change
        ))
        first_diaper_change_of_today = diapers_change_of_today[0]
        diapers_change_of_yesterday = list(filter(
            lambda diaper_change:
            diaper_change.start_millis >= start_of_yesterday and
            diaper_change.start_millis <= end_of_yesterday,
            diapers_change
        ))
        first_diaper_change_of_yesterday = diapers_change_of_yesterday[0]
        last_diaper_change_of_yesterday = diapers_change_of_yesterday[-1]
        day_period = {
            'start': first_diaper_change_of_yesterday.start_millis,
            'end': last_diaper_change_of_yesterday.start_millis
        }
        night_period = {
            'start': last_diaper_change_of_yesterday.start_millis,
            'end': first_diaper_change_of_today.start_millis
        }
        daily_naps_of_yesterday = list(filter(
            lambda nap:
            nap.start_millis > day_period['start'] and
            nap.end_millis < day_period['end'],
            sleeps
        ))
        night_sleeps_of_last_night = list(filter(
            lambda sleep:
            sleep.start_millis > night_period['start'] and
            sleep.end_millis < night_period['end'],
            sleeps
        ))
        for daily_nap_of_yesterday in daily_naps_of_yesterday:
            try:
                join[daily_nap_of_yesterday] = list(filter(
                    lambda nap:
                    nap.start_millis > daily_nap_of_yesterday.end_millis and
                    _convert_millis_to_minutes(
                        nap.start_millis - daily_nap_of_yesterday.end_millis) <= MAX_NAP_INTERVAL,
                    daily_naps_of_yesterday
                ))[0]
                if join[daily_nap_of_yesterday].group_uid != "":
                    wrong_groups[join[daily_nap_of_yesterday]] = ""
                if daily_nap_of_yesterday.group_uid != NAP_UID:
                    wrong_groups[daily_nap_of_yesterday] = NAP_UID
            except IndexError:
                continue
        for night_sleep_of_last_night in night_sleeps_of_last_night:
            try:
                join[night_sleep_of_last_night] = list(filter(
                    lambda sleep:
                    sleep.start_millis > night_sleep_of_last_night.end_millis and
                    _convert_millis_to_minutes(
                        sleep.start_millis - night_sleep_of_last_night.end_millis) <= MAX_SLEEP_INTERVAL,
                    night_sleeps_of_last_night
                ))[0]
                if join[night_sleep_of_last_night].group_uid != "":
                    wrong_groups[join[night_sleep_of_last_night]] = ""
                if night_sleep_of_last_night.group_uid != SLEEP_UID:
                    wrong_groups[night_sleep_of_last_night] = SLEEP_UID
            except IndexError:
                continue
    except IndexError:
        continue

print('-'*100)
print('Correção de grupos')
print('-'*100)
for sleep, group_uid in wrong_groups.items():
    sleep.group_uid = group_uid
    sleep.updated_millis = _get_now_in_millis()
    print(_calc_interval(sleep), 'mudará o grupo para', "\"" + group_uid + "\"")

print('-'*100)
print('Correção de gaps')
print('-'*100)
for sleep, gap in join.items():
    print(" Antes:", _calc_interval(sleep), "Intervalo (min)",
          _convert_millis_to_minutes(sleep.pause_millis))
    pause_millis = _convert_millis_to_minutes(
        gap.start_millis - sleep.end_millis) * 60 * 1000
    sleep.end_millis = gap.end_millis
    sleep.pause_millis += pause_millis + gap.pause_millis
    sleep.updated_millis = _get_now_in_millis()
    print("Depois:", _calc_interval(sleep), "Intervalo (min)",
          _convert_millis_to_minutes(sleep.pause_millis))
    print('-'*3)

# print(session.dirty)
# session.commit()
session.close()

with engine.connect() as connection:
    connection.execute("VACUUM")
