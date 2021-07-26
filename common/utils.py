import datetime
import config


def today(date=None):
    if date is not None:
        return date

    return datetime.date.today().strftime(config.DATE_FORMAT)


def tomorrow(date=today()):
    current = datetime.datetime.strptime(date, config.DATE_FORMAT)
    return (current + datetime.timedelta(days=1)).strftime(config.DATE_FORMAT)


def yesterday(date=today()):
    current = datetime.datetime.strptime(date, config.DATE_FORMAT)
    return (current + datetime.timedelta(days=-1)).strftime(config.DATE_FORMAT)


def last_month(date=today()):
    current = datetime.datetime.strptime(date, config.DATE_FORMAT)
    return (current + datetime.timedelta(days=-30)).strftime(config.DATE_FORMAT)


def last_year(date=today()):
    current = datetime.datetime.strptime(date, config.DATE_FORMAT)
    return (current + datetime.timedelta(days=-365)).strftime(config.DATE_FORMAT)