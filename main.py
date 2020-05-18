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

engine = create_engine('sqlite:///BabyDaybook.db')
Session = sessionmaker()
Session.configure(bind=engine)
session = Session()

Base = automap_base()
Base.prepare(engine, reflect=True)
Baby = Base.classes.babies
Action = Base.classes.daily_actions
Groups = Base.classes.groups

NAP_TITLE = 'Cochilo'
DIAPER_CHANGE_TYPE = 'diaper_change'
SLEEPING_TYPE = 'sleeping'

NAP_UID = session.query(Groups).filter_by(
    daily_action_type=SLEEPING_TYPE).filter_by(title=NAP_TITLE).first().uid
BABY_BDAY = pd.to_datetime(int(float(session.query(
    Baby).first().birthday)), unit='ms').tz_localize('UTC').tz_convert(TZ)

ALL_DAILY_ACTIONS = session.query(Action).order_by(Action.start_millis)
ALL_DIAPER_CHANGE = ALL_DAILY_ACTIONS.filter_by(type=DIAPER_CHANGE_TYPE)
ALL_SLEEPING = ALL_DAILY_ACTIONS.filter_by(type=SLEEPING_TYPE).filter(
    Action.start_millis < Action.end_millis)


def _convert_date_to_millis(date=None):
    return date.timestamp() * 1000


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


print('*'*100)
print('Sobreposição de Horários')

for sleep in ALL_SLEEPING.all():
    c = ALL_SLEEPING.filter(and_(Action.start_millis > sleep.start_millis,
                                 Action.start_millis <= sleep.end_millis)).count()
    if (c > 0):
        interval = _calc_interval(sleep)
        print(interval)

print('*'*100)
print('Corrigir sono noturno e cochilos')

diapers_change = ALL_DIAPER_CHANGE.all()
sleeps = ALL_SLEEPING.all()

all_sleeps = []
all_naps = []
wrong_sleeps_of_the_day = []
wrong_naps_of_the_day = []

for date in pd.date_range(start=BABY_BDAY, end=pd.Timestamp.today(tz=TZ), tz=TZ, normalize=True, closed='left'):
    start_of_the_day = _convert_date_to_millis(_get_start_of_the_day(date))
    end_of_the_day = _convert_date_to_millis(_get_end_of_the_day(date))

    diapers_change_of_the_day = list(filter(lambda diaper_change: diaper_change.start_millis >=
                                            start_of_the_day and diaper_change.start_millis <= end_of_the_day, diapers_change))

    try:
        first_diaper_change, last_diaper_change = diapers_change_of_the_day[
            0], diapers_change_of_the_day[-1]

        sleeps_of_the_day = list(filter(lambda sleep: (sleep.start_millis >= _convert_date_to_millis(date) and sleep.start_millis <= end_of_the_day) and (sleep.start_millis < first_diaper_change.start_millis or sleep.start_millis >
                                                                                                                                                          last_diaper_change.start_millis), sleeps))
        wrong_sleeps_of_the_day += list(
            filter(lambda sleep: sleep.group_uid != "", sleeps_of_the_day))

        naps_of_the_day = list(filter(lambda sleep: (sleep.start_millis > first_diaper_change.start_millis and sleep.start_millis <
                                                     last_diaper_change.start_millis), sleeps))
        wrong_naps_of_the_day += list(
            filter(lambda sleep: (sleep.group_uid != NAP_UID), naps_of_the_day))

        '''Acumulando para tratar posteriormente'''
        all_sleeps += sleeps_of_the_day
        all_naps += naps_of_the_day
    except:
        continue

print('*'*100)
print("Cochilos incorretos:")
for nap in wrong_naps_of_the_day:
    print(_convert_millis_to_date(nap.start_millis))
    nap.group_uid = NAP_UID

print('*'*100)
print("Dormidas incorretas:")
for sleep in wrong_sleeps_of_the_day:
    print(_convert_millis_to_date(sleep.start_millis))
    sleep.group_uid = ""

sleep_to_delete = []
sleep_to_change = {}

for sleep in all_sleeps:
    try:
        if (sleep in sleep_to_delete):
            continue

        next_sleep = list(filter(lambda s: (s.start_millis > sleep.end_millis) and (
            _convert_millis_to_minutes(s.start_millis - sleep.end_millis) <= 30), all_sleeps))[0]

        pause_millis = _convert_millis_to_minutes(
            next_sleep.start_millis - sleep.end_millis)*60*1000

        sleep_to_delete += [next_sleep]
        sleep_to_change[sleep] = [next_sleep.end_millis, pause_millis]
    except:
        pass

for sleep in all_naps:
    try:
        if (sleep in sleep_to_delete):
            continue

        next_sleep = list(filter(lambda s: (s.start_millis > sleep.end_millis) and (
            _convert_millis_to_minutes(s.start_millis - sleep.end_millis) <= 30), all_naps))[0]

        pause_millis = _convert_millis_to_minutes(
            next_sleep.start_millis - sleep.end_millis)*60*1000

        sleep_to_delete += [next_sleep]
        sleep_to_change[sleep] = [next_sleep.end_millis, pause_millis]
    except:
        pass

print('*'*100)
print("Excluindo dormidas intermediárias")
# print(sleep_to_delete)
[session.delete(sleep) for sleep in sleep_to_delete]

print('*'*100)
print("Extendendo dormidas interrompidas")
# print(sleep_to_change)
for sleep, value in sleep_to_change.items():
    sleep.end_millis = value[0]
    sleep.pause_millis += value[1]

print(session.dirty)
# session.commit()
session.close()
