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
NAP_TITLE = 'Cochilo'
SLEEP_TITLE = 'Sono da noite'
DIAPER_CHANGE_TYPE = 'diaper_change'
SLEEPING_TYPE = 'sleeping'
MAX_SLEEP_INTERVAL = 30
MAX_NAP_INTERVAL = 20

engine = create_engine('sqlite:///BabyDaybook.db')
Session = sessionmaker()
Session.configure(bind=engine)
session = Session()

Base = automap_base()
Base.prepare(engine, reflect=True)
Baby = Base.classes.babies
Action = Base.classes.daily_actions
Groups = Base.classes.groups

NAP_UID = session.query(Groups).filter_by(
    daily_action_type=SLEEPING_TYPE).filter_by(title=NAP_TITLE).first().uid
SLEEP_UID = session.query(Groups).filter_by(
    daily_action_type=SLEEPING_TYPE).filter_by(title=SLEEP_TITLE).first().uid
BABY_BDAY = pd.to_datetime(int(float(session.query(
    Baby).first().birthday)), unit='ms').tz_localize('UTC').tz_convert(TZ)

ALL_DAILY_ACTIONS = session.query(Action).order_by(Action.start_millis)


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


def _get_now_in_millis():
    return int(pd.Timestamp.today().timestamp()*1000)


diapers_change = ALL_DAILY_ACTIONS.filter_by(type=DIAPER_CHANGE_TYPE).all()
sleeps = ALL_DAILY_ACTIONS.filter_by(type=SLEEPING_TYPE).filter(
    Action.start_millis < Action.end_millis).all()

all_sleeps = []
all_naps = []
wrong_sleeps = []
wrong_naps = []
exclude = set()

'''Guardando listas de dormidas e cochilos'''
for date in pd.date_range(start=BABY_BDAY, end=pd.Timestamp.today(tz=TZ), tz=TZ, normalize=True, closed='left'):
    start_of_the_day = _convert_date_to_millis(_get_start_of_the_day(date))
    end_of_the_day = _convert_date_to_millis(_get_end_of_the_day(date))

    diapers_change_of_the_day = list(filter(lambda diaper_change: diaper_change.start_millis >= start_of_the_day and
                                            diaper_change.start_millis <= end_of_the_day,
                                            diapers_change))

    try:
        first_diaper_change = diapers_change_of_the_day[0]
        last_diaper_change = diapers_change_of_the_day[-1]

        sleeps_of_the_day = list(filter(lambda sleep: (sleep.start_millis >= _convert_date_to_millis(date) and
                                                       sleep.start_millis <= end_of_the_day) and
                                        (sleep.start_millis < first_diaper_change.start_millis or
                                         sleep.start_millis > last_diaper_change.start_millis), sleeps))
        wrong_sleeps += list(filter(lambda sleep: sleep.group_uid != SLEEP_UID,
                                    sleeps_of_the_day))

        naps_of_the_day = list(filter(lambda sleep: (sleep.start_millis > first_diaper_change.start_millis and
                                                     sleep.start_millis < last_diaper_change.start_millis),
                                      sleeps))
        wrong_naps += list(filter(lambda sleep: (sleep.group_uid != NAP_UID),
                                  naps_of_the_day))

        all_sleeps += sleeps_of_the_day
        all_naps += naps_of_the_day
    except:
        continue

all_sleeps_and_naps = list(set().union(all_sleeps, all_naps))

print('*'*100)
print('Removendo grupo das sobreposições de horário')
for sleep in all_sleeps_and_naps:
    nexts = list(filter(lambda next: (next.uid != sleep.uid) and
                        (next.group_uid == sleep.group_uid) and
                        (next.start_millis >= sleep.start_millis) and
                        (next.end_millis <= sleep.end_millis),
                        all_sleeps_and_naps))
    for next in nexts:
        next.group_uid = ""
        next.updated_millis = _get_now_in_millis()
        exclude.add(next)
        print(_calc_interval(sleep), _calc_interval(next))

print('*'*100)
print('Corrigindo grupo das dormidas')
for sleep in wrong_sleeps:
    if sleep in exclude:
        continue

    sleep.group_uid = SLEEP_UID
    sleep.updated_millis = _get_now_in_millis()
    print(_calc_interval(sleep), SLEEP_TITLE)

print('*'*100)
print('Corrigindo grupo dos cochilos')
for nap in wrong_naps:
    if nap in exclude:
        continue

    nap.group_uid = NAP_UID
    nap.updated_millis = _get_now_in_millis()
    print(_calc_interval(nap), NAP_TITLE)

correct_pause = dict()

print('*'*100)
print("Verificando quais dormidas ou cochilos podem ser mesclados")
for sleep in all_sleeps_and_naps:
    if sleep in exclude:
        continue

    try:
        next = list(filter(lambda next: (next.group_uid == sleep.group_uid) and
                           (next.start_millis > sleep.end_millis) and
                           (((_convert_millis_to_minutes(next.start_millis - sleep.end_millis) <= MAX_SLEEP_INTERVAL) and
                             (sleep.group_uid == SLEEP_UID)) or
                            ((_convert_millis_to_minutes(next.start_millis - sleep.end_millis) <= MAX_NAP_INTERVAL) and
                             (sleep.group_uid == NAP_UID))),
                           all_sleeps_and_naps))[0]

        pause_millis = _convert_millis_to_minutes(
            next.start_millis - sleep.end_millis) * 60 * 1000

        exclude.add(next)
        correct_pause[sleep] = [next.end_millis, pause_millis]
    except:
        pass

print('*'*100)
print("Mesclando dormidas intermediárias com interrupções")
for sleep, value in correct_pause.items():
    print("Antes: ", _calc_interval(sleep))
    sleep.end_millis = value[0]
    sleep.pause_millis += value[1]
    sleep.updated_millis = _get_now_in_millis()
    print("Depois: ", _calc_interval(sleep), "Intervalo (min)",
          _convert_millis_to_minutes(sleep.pause_millis))
    print()

print('*'*100)
print("Removendo grupo das dormidas intermediárias")
for sleep in exclude:
    print(_calc_interval(sleep))
    sleep.group_uid = ""
    sleep.updated_millis = _get_now_in_millis()

# print(session.dirty)
# session.commit()
session.close()

with engine.connect() as connection:
    connection.execute("VACUUM")
