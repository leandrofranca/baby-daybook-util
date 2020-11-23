import re
import sys
import uuid 
import time
import warnings

import mechanicalsoup
import pandas as pd
from sqlalchemy import and_, create_engine
from sqlalchemy.exc import SAWarning
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session, sessionmaker

warnings.filterwarnings('ignore', r".*support Decimal objects natively",
                        SAWarning, r"^sqlalchemy\.sql\.sqltypes$")

TZ = 'America/Fortaleza'
BABY_BDAY = pd.Timestamp(2019, 12, 20).tz_localize('UTC').tz_convert(TZ)
MAX_NAP_INTERVAL = 20
MAX_SLEEP_INTERVAL = 30

engine = create_engine('sqlite:///BabyDaybook.db')
Session = sessionmaker()
Session.configure(bind=engine)
session = Session()

Base = automap_base()
Base.prepare(engine, reflect=True)
Baby = Base.classes.babies
Action = Base.classes.daily_actions
Groups = Base.classes.groups


diapers = session.query(Action).filter_by(type='diaper_change').order_by(Action.start_millis).all()
groups = session.query(Groups).all()
sleeps = session.query(Action).filter_by(type='sleeping').filter(Action.start_millis < Action.end_millis).order_by(Action.start_millis).all()


def get_group_uid_from_title(title=None):
    return list(filter(
        lambda group:
        group.title == title,
        groups
    ))[0].uid


def get_group_title_from_uid(uid=None):
    return list(filter(
        lambda group:
        group.uid == uid,
        groups
    ))[0].title


foods = session.query(Action).filter_by(type='food').filter(Action.group_uid == get_group_uid_from_title('Fruta')).order_by(Action.start_millis).all()


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


groups = dict()
junctions = dict()
intersections = dict()

for date in pd.date_range(start=BABY_BDAY, end=pd.Timestamp.today(tz=TZ), tz=TZ, normalize=True, closed='left'):
    try:
        start_of_today = _convert_date_to_millis(_get_start_of_the_day(date))
        end_of_today = _convert_date_to_millis(_get_end_of_the_day(date))
        start_of_yesterday = _convert_date_to_millis(_get_start_of_the_day(_get_yesterday(date)))
        end_of_yesterday = _convert_date_to_millis(_get_end_of_the_day(_get_yesterday(date)))

        diapers_change_of_today = list(filter(
            lambda diaper_change:
            diaper_change.start_millis >= start_of_today and
            diaper_change.start_millis <= end_of_today,
            diapers
        ))
        first_diaper_change_of_today = diapers_change_of_today[0]
        diapers_change_of_yesterday = list(filter(
            lambda diaper_change:
            diaper_change.start_millis >= start_of_yesterday and
            diaper_change.start_millis <= end_of_yesterday,
            diapers
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
                if daily_nap_of_yesterday.group_uid != get_group_uid_from_title('Soneca'):
                    groups[daily_nap_of_yesterday] = get_group_uid_from_title('Soneca')
                intersections[daily_nap_of_yesterday] = list(filter(
                    lambda nap:
                    nap.start_millis > daily_nap_of_yesterday.start_millis and
                    nap.end_millis <= daily_nap_of_yesterday.end_millis and
                    (nap.pause_millis > 0 or nap.group_uid != ''),
                    daily_naps_of_yesterday
                ))[0]
                # if intersections[daily_nap_of_yesterday].group_uid != '':
                #     groups[intersections[daily_nap_of_yesterday]] = ''
            except IndexError:
                pass

            try:
                junctions[daily_nap_of_yesterday] = list(filter(
                    lambda nap:
                    nap.start_millis > daily_nap_of_yesterday.end_millis and
                    _convert_millis_to_minutes(nap.start_millis - daily_nap_of_yesterday.end_millis) <= MAX_NAP_INTERVAL,
                    daily_naps_of_yesterday
                ))[0]
                # if junctions[daily_nap_of_yesterday].group_uid != '':
                #     groups[junctions[daily_nap_of_yesterday]] = ''
            except IndexError:
                pass

        for night_sleep_of_last_night in night_sleeps_of_last_night:
            try:
                if night_sleep_of_last_night.group_uid != get_group_uid_from_title('Sono da noite'):
                    groups[night_sleep_of_last_night] = get_group_uid_from_title('Sono da noite')
                intersections[night_sleep_of_last_night] = list(filter(
                    lambda sleep:
                    sleep.start_millis > night_sleep_of_last_night.start_millis and
                    sleep.end_millis <= night_sleep_of_last_night.end_millis and
                    (sleep.pause_millis > 0 or sleep.group_uid != ''),
                    night_sleeps_of_last_night
                ))[0]
                # if intersections[night_sleep_of_last_night].group_uid != '':
                #     groups[intersections[night_sleep_of_last_night]] = ''
            except IndexError:
                pass

            try:
                junctions[night_sleep_of_last_night] = list(filter(
                    lambda sleep:
                    sleep.start_millis > night_sleep_of_last_night.end_millis and
                    _convert_millis_to_minutes(sleep.start_millis - night_sleep_of_last_night.end_millis) <= MAX_SLEEP_INTERVAL,
                    night_sleeps_of_last_night
                ))[0]
                # if junctions[night_sleep_of_last_night].group_uid != '':
                #     groups[junctions[night_sleep_of_last_night]] = ''
            except IndexError:
                pass
    except IndexError:
        continue

print('-'*100)
print('Transferindo interrupções para dormida principal')
print('-'*100)
for sleep, internal in intersections.items():
    print(' Inicial:', _calc_interval(sleep), 'intervalo (min)', _convert_millis_to_minutes(sleep.pause_millis), '+')
    print(' Interna:', _calc_interval(internal), 'intervalo (min)', _convert_millis_to_minutes(internal.pause_millis), '=')

    sleep.pause_millis += internal.pause_millis
    sleep.updated_millis = _get_now_in_millis()

    print('   Final:', _calc_interval(sleep), 'intervalo (min)', _convert_millis_to_minutes(sleep.pause_millis))
    print('-'*3)

print('-'*100)
print('Removendo pausas das interseções')
print('-'*100)
for sleep in intersections.values():
    print(' Antes:', _calc_interval(sleep), 'intervalo (min)', _convert_millis_to_minutes(sleep.pause_millis))

    sleep.pause_millis = 0
    sleep.updated_millis = _get_now_in_millis()

    print('Depois:', _calc_interval(sleep), 'intervalo (min)', _convert_millis_to_minutes(sleep.pause_millis))
    print('-'*3)

print('-'*100)
print('Correção de grupos')
print('-'*100)
for sleep, group_uid in groups.items():
    if sleep.group_uid != group_uid:
        print(_calc_interval(sleep), get_group_title_from_uid(sleep.group_uid), get_group_title_from_uid(group_uid))
        sleep.group_uid = group_uid
        sleep.updated_millis = _get_now_in_millis()

print('-'*100)
print('Correção de gaps')
print('-'*100)
for sleep, gap in junctions.items():
    print(' Antes:', _calc_interval(sleep), 'intervalo (min)', _convert_millis_to_minutes(sleep.pause_millis))
    pause_millis = _convert_millis_to_minutes(gap.start_millis - sleep.end_millis) * 60 * 1000
    sleep.end_millis = gap.end_millis
    sleep.pause_millis += pause_millis + gap.pause_millis
    sleep.updated_millis = _get_now_in_millis()
    print('Depois:', _calc_interval(sleep), 'intervalo (min)', _convert_millis_to_minutes(sleep.pause_millis))
    print('-'*3)

print('-'*100)
print('Transformação de Fruta em Lanche')
print('-'*100)
for f in foods:
    print(_convert_millis_to_date(f.start_millis), get_group_title_from_uid(f.group_uid), "Lanche")
    f.group_uid = get_group_uid_from_title('Lanche')
    f.updated_millis = _get_now_in_millis()

print('-'*100)
print('Trazendo informação da creche')
print('-'*100)
browser = mechanicalsoup.StatefulBrowser()
browser.open("https://responsavel.agendaedu.com/")
browser.select_form('form[action="/responsibles/sign_in"]')
browser["responsible[login]"] = ""
browser["responsible[password]"] = ""
browser.submit_selected()
p = re.compile('(\\d{2}:\\d{2})')
for date in pd.date_range(start=pd.Timestamp('2020-09-14', tz='America/Fortaleza'), end=pd.Timestamp.today(tz='America/Fortaleza'), freq='B', tz='America/Fortaleza'):
    browser.open("https://responsavel.agendaedu.com/responsibles/daily_summaries/%s" % date.strftime('%Y-%m-%d'))
    page = browser.get_current_page().getText()
    result = p.findall(page)
    if len(result) >= 4:
        print(date.strftime('%Y-%m-%d'), 'uid', uuid.uuid1().hex, 'user_uid', 'S2gBcXYovNfOAR4lflCUGEGH5vr2', 'baby_uid', 'uNWEPUD7531142628466708417801218',
              'type', 'sleeping', 'start_millis', 'end_millis', 'group_uid', get_group_uid_from_title('Soneca'), 'updated_millis', _get_now_in_millis())
        print(result)


# session.commit()
session.close()

with engine.connect() as connection:
    connection.execute('VACUUM')
