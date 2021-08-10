import datetime
import config


def today(date=None):
    if date is not None:
        return date

    return datetime.date.today().strftime(config.DATE_FORMAT)


def tomorrow(date=today()):
    return time_delta(1, date)


def yesterday(date=today()):
    return time_delta(-1, date)


def last_week(date=today()):
    return time_delta(-7, date)


def last_month(date=today()):
    return time_delta(-30, date)


def last_year(date=today()):
    return time_delta(-365, date)


def time_delta(days, date=today()):
    current = parse_datetime(date)
    return (current + datetime.timedelta(days=days)).strftime(config.DATE_FORMAT)


def parse_datetime(str_date):
    return datetime.datetime.strptime(str_date, config.DATE_FORMAT)
