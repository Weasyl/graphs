import json
import sys

from matplotlib.dates import MonthLocator, DateFormatter, date2num
from matplotlib import pyplot as plt
import psycopg2


RATINGS = [10, 20, 30, 40]
COLORS = ['black', 'teal', 'yellow', 'red']


def submissions(curs):
    curs.execute("""
        SELECT *
        FROM
            (SELECT
                 date_trunc('day', to_timestamp(unixtime)) AS day,
                 rating,
                 count(*) count
             FROM submission
             GROUP BY day, rating) subq
        WHERE day >= '2012-10-01'::timestamp
    """)
    results = {}
    for day, rating, count in curs:
        results.setdefault(date2num(day), {})[rating] = count

    days = []
    lower_bounds = {r: [] for r in RATINGS}
    upper_bounds = {r: [] for r in RATINGS}
    for day, counts in sorted(results.items()):
        days.append(day)
        for rating in RATINGS:
            last = (upper_bounds.get(rating - 10) or [0])[-1]
            lower_bounds[rating].append(last)
            upper_bounds[rating].append(counts.get(rating, 0) + last)


    fig, axes = plt.subplots(figsize=(24,8))
    axes.set_xlim(days[0], days[-1])
    for rating, color in zip(RATINGS, COLORS):
        axes.fill_between(days, lower_bounds[rating], upper_bounds[rating], facecolor=color, linewidth=0)
    axes.xaxis.set_major_locator(MonthLocator())
    axes.xaxis.set_major_formatter(DateFormatter('%Y-%m-%d'))
    axes.grid(True)
    fig.autofmt_xdate()
    plt.savefig('submissions.png')


def users(curs):
    curs.execute("""
        SELECT *
        FROM
            (SELECT
                 date_trunc('day', to_timestamp(unixtime)) AS day,
                 count(*) count
             FROM profile
             GROUP BY day) subq
        WHERE day >= '2012-10-01'::timestamp
        ORDER BY day ASC
    """)
    days = []
    zeroes = []
    counts = []
    for day, count in curs:
        days.append(date2num(day))
        zeroes.append(0)
        counts.append(count)

    fig, axes = plt.subplots(figsize=(24,8))
    axes.set_xlim(days[0], days[-1])
    axes.fill_between(days, zeroes, counts, facecolor='teal', linewidth=0)
    axes.xaxis.set_major_locator(MonthLocator())
    axes.xaxis.set_major_formatter(DateFormatter('%Y-%m-%d'))
    axes.grid(True)
    fig.autofmt_xdate()
    plt.savefig('users.png')


def main():
    config = json.load(sys.stdin)
    conn = psycopg2.connect(**config)
    curs = conn.cursor()
    submissions(curs)
    users(curs)


if __name__ == '__main__':
    main()
