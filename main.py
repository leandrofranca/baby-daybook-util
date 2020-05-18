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
SLEEP_TITLE = 'Sono da noite'
DELETE_SLEEP_TITLE = 'Excluir'
DIAPER_CHANGE_TYPE = 'diaper_change'
SLEEPING_TYPE = 'sleeping'

NAP_UID = session.query(Groups).filter_by(
    daily_action_type=SLEEPING_TYPE).filter_by(title=NAP_TITLE).first().uid
SLEEP_UID = session.query(Groups).filter_by(
    daily_action_type=SLEEPING_TYPE).filter_by(title=SLEEP_TITLE).first().uid
DELETE_SLEEP_UID = session.query(Groups).filter_by(
    daily_action_type=SLEEPING_TYPE).filter_by(title=DELETE_SLEEP_TITLE).first().uid
BABY_BDAY = pd.to_datetime(int(float(session.query(
    Baby).first().birthday)), unit='ms').tz_localize('UTC').tz_convert(TZ)

ALL_DAILY_ACTIONS = session.query(Action).order_by(Action.start_millis)
ALL_DIAPER_CHANGE = ALL_DAILY_ACTIONS.filter_by(type=DIAPER_CHANGE_TYPE)
ALL_SLEEPING = ALL_DAILY_ACTIONS.filter_by(type=SLEEPING_TYPE).filter(
    Action.start_millis < Action.end_millis)


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


to_delete = set()
to_update = {}

print('*'*100)
print('Sobreposição de horários (marcando para exclusão)')

for sleep in ALL_SLEEPING.all():
    nexts = ALL_SLEEPING.filter(and_(Action.uid != sleep.uid,
                                     Action.start_millis >= sleep.start_millis,
                                     Action.end_millis <= sleep.end_millis)).all()
    for next in nexts:
        to_delete.add(next)
        print(_calc_interval(sleep), _calc_interval(next))

print('*'*100)
print("Corrigindo grupos")

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
            filter(lambda sleep: sleep.group_uid != SLEEP_UID, sleeps_of_the_day))

        naps_of_the_day = list(filter(lambda sleep: (sleep.start_millis > first_diaper_change.start_millis and sleep.start_millis <
                                                     last_diaper_change.start_millis), sleeps))
        wrong_naps_of_the_day += list(
            filter(lambda sleep: (sleep.group_uid != NAP_UID), naps_of_the_day))

        '''Acumulando para tratar posteriormente'''
        all_sleeps += sleeps_of_the_day
        all_naps += naps_of_the_day
    except:
        continue

for nap in wrong_naps_of_the_day:
    if nap in to_delete:
        continue
    nap.group_uid = NAP_UID
    nap.updated_millis = int(pd.Timestamp.today().timestamp()*1000)
    print(_calc_interval(nap))
for sleep in wrong_sleeps_of_the_day:
    if sleep in to_delete:
        continue
    sleep.group_uid = SLEEP_UID
    sleep.updated_millis = int(pd.Timestamp.today().timestamp()*1000)
    print(_calc_interval(sleep))

print('*'*100)
print("Transformando dormidas intermediárias em uma única com interrupções")
for sleep in all_sleeps:
    try:
        if sleep in to_delete:
            continue

        next = list(filter(lambda s: (s.start_millis > sleep.end_millis) and (
            _convert_millis_to_minutes(s.start_millis - sleep.end_millis) <= 30), all_sleeps))[0]

        pause_millis = _convert_millis_to_minutes(
            next.start_millis - sleep.end_millis)*60*1000

        to_delete.add(next)
        to_update[sleep] = [next.end_millis, pause_millis]
    except:
        pass

for nap in all_naps:
    try:
        if nap in to_delete:
            continue

        next = list(filter(lambda s: (s.start_millis > nap.end_millis) and (
            _convert_millis_to_minutes(s.start_millis - nap.end_millis) <= 20), all_naps))[0]

        pause_millis = _convert_millis_to_minutes(
            next.start_millis - nap.end_millis)*60*1000

        to_delete.add(next)
        to_update[nap] = [next.end_millis, pause_millis]
    except:
        pass

for sleep, value in to_update.items():
    print("Antes: ", _calc_interval(sleep))
    sleep.end_millis = value[0]
    sleep.pause_millis += value[1]
    sleep.updated_millis = int(pd.Timestamp.today().timestamp()*1000)
    print("Depois: ", _calc_interval(sleep), "Intervalo (min)",
          _convert_millis_to_minutes(sleep.pause_millis))
    print()

print('*'*100)
print("Eventos marcados para exclusão")
for sleep in to_delete:
    print(_calc_interval(sleep))
    sleep.group_uid = DELETE_SLEEP_UID
    sleep.updated_millis = int(pd.Timestamp.today().timestamp()*1000)

# print(session.dirty)
# session.commit()
session.close()

with engine.connect() as connection:
    connection.execute("VACUUM")
