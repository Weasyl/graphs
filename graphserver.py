import datetime
import decimal
import json
import sys
import time

import treq
from twisted.internet import endpoints, defer, task
from twisted.web import resource, server, static
from txpostgres import txpostgres


QUERIES = {
    'reports_by_day': """
        SELECT *
        FROM
            (SELECT
                 date_trunc('day', to_timestamp(unixtime)) AS day,
                 count(*) count
             FROM report
             GROUP BY day) subq
        WHERE day >= '2012-10-01'::timestamp
    """,
    'submissions_by_day_and_rating': """
        SELECT *
        FROM
            (SELECT
                 date_trunc('day', to_timestamp(unixtime)) AS day,
                 rating,
                 count(*) count
             FROM submission
             GROUP BY day, rating) subq
        WHERE day >= '2012-10-01'::timestamp
    """,
    'submissions_by_day_and_type': """
        SELECT *
        FROM
            (SELECT
                 date_trunc('day', to_timestamp(unixtime)) AS day,
                 subtype / 1000 typ,
                 count(*) count
             FROM submission
             GROUP BY day, typ) subq
        WHERE day >= '2012-10-01'::timestamp
    """,
    'users_by_day': """
        SELECT *
        FROM
            (SELECT
                 date_trunc('day', to_timestamp(unixtime)) AS day,
                 count(*) count
             FROM profile
             GROUP BY day) subq
        WHERE day >= '2012-10-01'::timestamp
        ORDER BY day ASC
    """,
    'main_genders': """
        WITH genders 
             AS (SELECT CASE 
                          WHEN lower(gender) IN ('male', 'female') THEN lower(gender) 
                          ELSE 'other' 
                        END gender 
                 FROM   userinfo 
                 WHERE  gender != '') 
        SELECT gender, 
               count(*) 
        FROM   genders 
        GROUP  BY gender 
    """,
    'other_genders': """
        WITH genders 
             AS (SELECT lower(gender) gender 
                 FROM   userinfo 
                 WHERE  lower(gender) NOT IN ('male', 'female', '')) 
        SELECT gender, 
               count(*) 
        FROM   genders 
        GROUP  BY gender 
        ORDER  BY count(*) DESC 
        LIMIT  25
    """,
    'genders': """
        SELECT lower(gender), COUNT(*)
          FROM userinfo
         WHERE gender != ''
         GROUP BY lower(gender)
         ORDER BY COUNT(*) DESC
         LIMIT 25
    """,
    'ages': """
        WITH ages AS (
            SELECT date_trunc('year', age(to_timestamp(birthday))) age, COUNT(*)
              FROM userinfo
             GROUP BY age
             ORDER BY age ASC
        ) SELECT * FROM ages WHERE age > '0 minutes'
    """,
    'tickets': """
        SELECT CASE
                 WHEN report.settings = 'r'
                       OR login_name IS NULL THEN 'open-ticket'
                 ELSE login_name
               END                                         reporter,
               date_trunc('month', to_timestamp(unixtime)) opened_at,
               count(*)                                    count
        FROM   report
               LEFT JOIN login
                      ON report.closerid = login.userid
        GROUP  BY opened_at,
                  reporter
    """,
    'tag_counts': """
        WITH n_tags 
             AS (SELECT count(*) n_tags 
                 FROM   searchmapsubmit 
                 GROUP  BY targetid) 
        SELECT n_tags, 
               count(*) 
        FROM   n_tags 
        GROUP  BY n_tags
    """,
    'tag_popularity': """
        SELECT title, 
               count(*) 
        FROM   searchmapsubmit 
               JOIN searchtag USING (tagid) 
        GROUP  BY title 
        ORDER  BY count(*) DESC 
        LIMIT  25 
    """,
    'tag_favorites': """
        WITH fave_counts 
             AS (SELECT targetid submitid, 
                        count(*) favorites 
                 FROM   favorite 
                 WHERE  type = 's' 
                 GROUP  BY targetid) 
        SELECT searchtag.title, 
               sum(favorites) 
        FROM   searchmapsubmit 
               JOIN searchtag using (tagid) 
               JOIN submission 
                 ON targetid = submitid 
               JOIN fave_counts using (submitid) 
        GROUP  BY searchtag.title 
        ORDER  BY sum(favorites) DESC 
        LIMIT  25
    """,
    'other_rating_statistics': """
        WITH fave_counts 
             AS (SELECT targetid submitid, 
                        count(*) favorites 
                 FROM   favorite 
                 WHERE  type = 's' 
                 GROUP  BY targetid) 
        SELECT rating, 
               sum(page_views), 
               sum(favorites), 
               sum(page_views::float) / count(*), 
               sum(favorites) / count(*), 
               sum(favorites) / sum(page_views) 
        FROM   submission 
               JOIN fave_counts using (submitid) 
        GROUP  BY rating 
    """,
    'tag_popularity_favorites': """
        WITH fave_counts 
             AS (SELECT targetid submitid, 
                        count(*) favorites 
                 FROM   favorite 
                 WHERE  type = 's' 
                 GROUP  BY targetid) 
        SELECT searchtag.title, 
               count(*), 
               sum(favorites) 
        FROM   searchmapsubmit 
               JOIN searchtag using (tagid) 
               JOIN submission 
                 ON targetid = submitid 
               JOIN fave_counts using (submitid) 
        GROUP  BY searchtag.title 
        ORDER  BY count(*) DESC 
        LIMIT  100
    """,
    'tag_interest': """
        WITH fave_counts 
             AS (SELECT targetid submitid, 
                        count(*) favorites 
                 FROM   favorite 
                 WHERE  type = 's' 
                 GROUP  BY targetid), 
             avg_faves 
             AS (SELECT sum(favorites) / count(*) avg_faves 
                 FROM   fave_counts), 
             total_submissions 
             AS (SELECT count(*) total_submissions 
                 FROM   submission) 
        SELECT searchtag.title, 
               count(*)::float / total_submissions, 
               sum(( favorites > avg_faves )::int::float) / count(*) 
        FROM   avg_faves, 
               total_submissions, 
               searchmapsubmit 
               JOIN searchtag using (tagid) 
               JOIN submission 
                 ON targetid = submitid 
               JOIN fave_counts using (submitid) 
        GROUP  BY searchtag.title, 
                  total_submissions 
        ORDER  BY count(*) DESC 
        LIMIT  100
    """,
    'submission_pct_views_favorites_comments_submissions_by_rating': """
        WITH favorites 
             AS (SELECT targetid submitid, 
                        count(*) favorites 
                 FROM   favorite 
                 WHERE  type = 's' 
                 GROUP  BY targetid), 
             comments 
             AS (SELECT target_sub submitid, 
                        count(*)   AS comments 
                 FROM   comments 
                 WHERE  target_sub IS NOT NULL 
                 GROUP  BY target_sub), 
             totals 
             AS (SELECT sum(page_views) all_page_views, 
                        sum(favorites)  all_favorites, 
                        sum(comments)   all_comments, 
                        count(*)        AS all_submissions 
                 FROM   submission 
                        JOIN favorites using (submitid) 
                        JOIN comments using (submitid)) 
        SELECT rating, 
               sum(page_views)::float / all_page_views, 
               sum(favorites)::float / all_favorites, 
               sum(comments)::float / all_comments, 
               count(*)::float / all_submissions 
        FROM   totals, 
               submission 
               JOIN favorites using (submitid) 
               JOIN comments using (submitid) 
        GROUP  BY rating, 
                  all_page_views, 
                  all_favorites, 
                  all_comments, 
                  all_submissions 
    """,
    'follows_vs_submissions': """
        WITH follows 
             AS (SELECT otherid  userid, 
                        count(*) follows 
                 FROM   watchuser 
                 GROUP  BY otherid), 
             submissions 
             AS (SELECT userid, 
                        count(*) submissions 
                 FROM   submission 
                 GROUP  BY userid) 
        SELECT username, 
               submissions, 
               follows, 
               CASE 
                 WHEN submissions > follows THEN submissions::float / max_submissions 
                 ELSE follows::float / max_follows 
               END score 
        FROM   (SELECT max(submissions) max_submissions 
                FROM   submissions) subq1, 
               (SELECT max(follows) max_follows 
                FROM   follows) subq2, 
               follows 
               JOIN submissions using (userid) 
               JOIN profile using (userid)
        ORDER  BY score DESC 
        LIMIT  250
    """,
    'follows_vs_submissions_clustered': """
        WITH follows 
             AS (SELECT otherid  userid, 
                        count(*) follows 
                 FROM   watchuser 
                 GROUP  BY otherid), 
             submissions 
             AS (SELECT userid, 
                        count(*) submissions 
                 FROM   submission 
                 GROUP  BY userid) 
        SELECT min(COALESCE(submissions, 0)), 
               min(COALESCE(follows, 0)), 
               count(*) 
        FROM   login 
               LEFT JOIN follows using (userid) 
               LEFT JOIN submissions using (userid) 
        GROUP  BY COALESCE(submissions, 0) / 40, 
                  COALESCE(follows, 0) / 100 
        ORDER  BY count(*) DESC 
    """,
}


def serializeOther(obj):
    if isinstance(obj, datetime.datetime):
        return obj.replace(tzinfo=None).isoformat()
    elif isinstance(obj, datetime.timedelta):
        return obj.total_seconds()
    elif isinstance(obj, decimal.Decimal):
        return float(obj)
    else:
        raise TypeError("can't serialize", obj)


class QueriesResource(resource.Resource):
    cacheLength = 60 * 60 * 24

    def __init__(self, conn, piwikToken):
        resource.Resource.__init__(self)
        self.conn = conn
        self.piwikToken = piwikToken
        self.cache = {}

    def render_GET(self, request):
        request.setHeader('Content-Type', 'application/json')
        query = request.args.get('query', [None])[0]
        if not query:
            return json.dumps({'error': 'bad query'})
        meth = getattr(self, 'query_' + query, None)
        if meth is None and query not in QUERIES:
            return json.dumps({'error': 'bad query'})

        if query in self.cache:
            cachedAt, results = self.cache[query]
            now = time.time()
            if cachedAt + self.cacheLength > now:
                self._reply(results, request)
                return server.NOT_DONE_YET

        if meth is None:
            d = self.conn.runQuery(QUERIES[query])
        else:
            d = meth()
        d.addCallback(self._reply, request)
        d.addCallback(self._cache, query)
        d.addErrback(request.processingFailed)
        return server.NOT_DONE_YET

    def _reply(self, results, request):
        request.write(json.dumps({'result': results}, default=serializeOther))
        request.finish()
        return results

    def _cache(self, results, query):
        self.cache[query] = time.time(), results

    @defer.inlineCallbacks
    def query_favorites_vs_view_time(self):
        resp = yield treq.get(
            'https://www.weasyl.com/piwik/index.php?module=API&method=Actions.getPageUrls'
            '&idSite=1&period=day&date=yesterday&format=json&idSubtable=29&depth=200&token_auth=' + self.piwikToken.encode())
        j = yield treq.json_content(resp)
        submissions = [int(row['label']) for row in j if row['label'].isdigit()]
        favorites = yield self.conn.runQuery("""
            SELECT targetid submitid, 
                   count(*) favorites 
            FROM   favorite 
            WHERE  type = 's' 
                   AND targetid IN %s 
            GROUP  BY targetid 
        """, (tuple(submissions),))
        favorites = dict(favorites)
        defer.returnValue([[submitid, row['avg_time_on_page'], row['nb_visits'], favorites.get(submitid, 0)] for row, submitid in zip(j, submissions)])


@defer.inlineCallbacks
def main(reactor, config, description):
    with open(config, 'rb') as infile:
        configObj = json.load(infile)

    conn = txpostgres.Connection(reactor=reactor)
    yield conn.connect(**configObj['postgres'])

    rootResource = resource.Resource()
    rootResource.putChild('query', QueriesResource(conn, configObj['piwik']))
    rootResource.putChild('static', static.File('static'))

    endpoint = endpoints.serverFromString(reactor, description)
    yield endpoint.listen(server.Site(rootResource))
    yield defer.Deferred()


task.react(main, sys.argv[1:])
