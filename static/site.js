d3.json('/query?query=main_genders', function (error, j) {
    var data = j.result.map(function (v) {
        return {'gender': v[0], 'count': v[1]};
    });
    var svg = dimple.newSvg('#main-genders', 800, 300);
    var chart = new dimple.chart(svg, data);
    chart.setBounds(120, 30, 660, 200)
    chart.addMeasureAxis('x', 'count');
    chart.addCategoryAxis('y', 'gender');
    chart.addSeries(null, dimple.plot.bar);
    chart.draw();
});

d3.json('/query?query=other_genders', function (error, j) {
    var data = j.result.map(function (v) {
        return {'gender': v[0], 'count': v[1]};
    });
    var svg = dimple.newSvg('#other-genders', 800, 600);
    var chart = new dimple.chart(svg, data);
    chart.setBounds(120, 30, 660, 500)
    chart.addMeasureAxis('x', 'count').tickFormat = 'd';
    chart.addCategoryAxis('y', 'gender');
    chart.addSeries(null, dimple.plot.bar);
    chart.draw();
});

d3.json('/query?query=ages', function (error, j) {
    var data = j.result.map(function (v) {
        return [v[0] / 60 / 60 / 24 / 365, v[1]];
    });
    new Dygraph(
        'ages',
        data,
        {
            labels: ['age', 'users'],
            xlabel: 'Age',
            ylabel: 'Number of users',
        });
});

var dateFormat = d3.time.format('%Y-%m-%dT%H:%M:%S');

d3.json('/query?query=users_by_day', function (error, j) {
    var data = j.result.map(function (v) {
        return [dateFormat.parse(v[0]), v[1]];
    });
    new Dygraph(
        'users',
        data,
        {
            labels: ['date', 'users'],
            xlabel: 'Registration date',
            ylabel: 'Number of users',
        });
});

var ratings = d3.map({
    '10': 'general',
    '20': 'moderate',
    '30': 'mature',
    '40': 'explicit',
});
var ratingOrder = ['general', 'moderate', 'mature', 'explicit'];

function aggregateByDate(input, columnOrder) {
    var dataByDate = d3.map();
    input.forEach(function (v) {
        var target = dataByDate.get(v[0]);
        if (!target) {
            target = d3.map();
            dataByDate.set(v[0], target);
        }
        target.set(v[1], v[2]);
    });
    var data = [];
    dataByDate.forEach(function (k, v) {
        var date = dateFormat.parse(k);
        var row = [date];
        columnOrder.forEach(function (col) {
            row.push(v.get(col));
        });
        data.push(row);
    });
    data.sort(function (a, b) { return d3.ascending(a[0], b[0]); });
    return data;
}

d3.json('/query?query=submissions_by_day_and_rating', function (error, j) {
    var data = aggregateByDate(j.result.map(function (v) { 
        return [v[0], ratings.get(v[1]), v[2]];
    }), ratingOrder);
    new Dygraph(
        'submissions',
        data,
        {
            labels: ['date'].concat(ratingOrder),
            xlabel: 'Submission date',
            ylabel: 'Number of submissions',
            stackedGraph: true,
            legend: 'always',
        });
});

function percentageOfTotal(input) {
    return input.map(function (row) {
        var rest = row.slice(1);
        var total = d3.sum(rest);
        return [row[0]].concat(rest.map(function (v) { return (v || 0) / total; }));
    });
}

d3.json('/query?query=submissions_by_day_and_rating', function (error, j) {
    var data = aggregateByDate(j.result.map(function (v) { 
        return [v[0], ratings.get(v[1]), v[2]];
    }), ratingOrder);
    data = percentageOfTotal(data);
    new Dygraph(
        'submissions-by-rating',
        data,
        {
            labels: ['date'].concat(ratingOrder),
            xlabel: 'Submission date',
            ylabel: 'Percentage of total submissions',
            stackedGraph: true,
            legend: 'always',
            rollPeriod: 20,
            axes: {
                y: {
                    valueFormatter: d3.format('.3p'),
                },
            },
        });
});

var submissionTypes = d3.map({
    '1': 'visual',
    '2': 'literary',
    '3': 'multimedia',
});
var submissionTypeOrder = ['visual', 'literary', 'multimedia'];

d3.json('/query?query=submissions_by_day_and_type', function (error, j) {
    var data = aggregateByDate(j.result.map(function (v) { 
        return [v[0], submissionTypes.get(v[1]), v[2]];
    }), submissionTypeOrder);
    data = percentageOfTotal(data);
    new Dygraph(
        'submissions-by-type',
        data,
        {
            labels: ['date'].concat(submissionTypeOrder),
            xlabel: 'Submission date',
            ylabel: 'Percentage of total submissions',
            stackedGraph: true,
            legend: 'always',
            rollPeriod: 20,
            axes: {
                y: {
                    valueFormatter: d3.format('.3p'),
                },
            },
        });
});

d3.json('/query?query=tag_popularity', function (error, j) {
    var data = j.result.map(function (v) {
        return {'tag': v[0], 'submissions': v[1]};
    });
    var svg = dimple.newSvg('#popular-tags', 800, 600);
    var chart = new dimple.chart(svg, data);
    chart.setBounds(120, 30, 660, 500)
    chart.addMeasureAxis('x', 'submissions');
    chart.addCategoryAxis('y', 'tag');
    chart.addSeries(null, dimple.plot.bar);
    chart.draw();
});

d3.json('/query?query=tag_favorites', function (error, j) {
    var data = j.result.map(function (v) {
        return {'tag': v[0], 'favorites': v[1]};
    });
    var svg = dimple.newSvg('#tag-favorites', 800, 600);
    var chart = new dimple.chart(svg, data);
    chart.setBounds(120, 30, 660, 500)
    chart.addMeasureAxis('x', 'favorites');
    chart.addCategoryAxis('y', 'tag');
    chart.addSeries(null, dimple.plot.bar);
    chart.draw();
});

d3.json('/query?query=tag_popularity_favorites', function (error, j) {
    var data = j.result.map(function (v) {
        return {'tag': v[0], 'submissions': v[1], 'favorites': v[2]};
    });
    data.sort(function (a, b) { return d3.descending(a['submissions'], b['submissions']) });

    var svg = dimple.newSvg('#tag-use-favorites', 800, 600);
    var chart = new dimple.chart(svg, data.slice(20));
    chart.setBounds(120, 30, 660, 500)
    chart.addMeasureAxis('x', 'favorites');
    chart.addMeasureAxis('y', 'submissions');
    chart.addSeries(['tag'], dimple.plot.bubble);
    chart.draw();

    var svg = dimple.newSvg('#tag-use-favorites-full', 800, 600);
    var chart = new dimple.chart(svg, data);
    chart.setBounds(120, 30, 660, 500)
    chart.addMeasureAxis('x', 'favorites');
    chart.addMeasureAxis('y', 'submissions');
    chart.addSeries(['tag'], dimple.plot.bubble);
    chart.draw();
});

d3.json('/query?query=tag_interest', function (error, j) {
    var data = j.result.map(function (v) {
        return {'tag': v[0], 'percentage of submissions with tag': v[1], 'percentage of tagged submissions favorited': v[2]};
    });
    data.sort(function (a, b) { return d3.descending(a['percentage of submissions with tag'], b['percentage of submissions with tag']) });

    var svg = dimple.newSvg('#tag-interest', 800, 600);
    var chart = new dimple.chart(svg, data.slice(20));
    chart.setBounds(120, 30, 660, 500)
    chart.addMeasureAxis('x', 'percentage of tagged submissions favorited').tickFormat = '.3p';
    chart.addMeasureAxis('y', 'percentage of submissions with tag').tickFormat = '.3p';
    chart.addSeries(['tag'], dimple.plot.bubble);
    chart.draw();

    var svg = dimple.newSvg('#tag-interest-full', 800, 600);
    var chart = new dimple.chart(svg, data);
    chart.setBounds(120, 30, 660, 500)
    chart.addMeasureAxis('x', 'percentage of tagged submissions favorited').tickFormat = '.3p';
    chart.addMeasureAxis('y', 'percentage of submissions with tag').tickFormat = '.3p';
    chart.addSeries(['tag'], dimple.plot.bubble);
    chart.draw();
});

d3.json('/query?query=other_rating_statistics', function (error, j) {
    var data = j.result;
    data.sort(function (a, b) { return d3.ascending(a[0], b[0]); });
    var newData = [];
    data.forEach(function (v) {
        var rating = ratings.get(v[0]);
        newData.push({rating: rating, category: 'page views', count: v[3]});
        newData.push({rating: rating, category: 'favorites', count: v[4]});
    });
    var svg = dimple.newSvg('#rating-views-favorites', 800, 600);
    var chart = new dimple.chart(svg, newData);
    chart.setBounds(120, 30, 660, 500)
    chart.addCategoryAxis('y', ['rating', 'category']);
    chart.addMeasureAxis('x', 'count');
    chart.addSeries('category', dimple.plot.bar);
    chart.addLegend(65, 10, 510, 20, 'right');
    chart.draw();
});

d3.json('/query?query=submission_pct_views_favorites_comments_submissions_by_rating', function (error, j) {
    var data = j.result;
    data.sort(function (a, b) { return d3.ascending(a[0], b[0]); });
    var newData = [];
    data.forEach(function (v) {
        var rating = ratings.get(v[0]);
        newData.push({rating: rating, category: 'percent of total page views', percent: v[1]});
        newData.push({rating: rating, category: 'percent of total favorites', percent: v[2]});
        newData.push({rating: rating, category: 'percent of total comments', percent: v[3]});
        newData.push({rating: rating, category: 'percent of all submissions', percent: v[4]});
    });
    var svg = dimple.newSvg('#rating-pct-views-favorites-comments-submissions', 800, 600);
    var chart = new dimple.chart(svg, newData);
    chart.setBounds(120, 30, 660, 500)
    chart.addCategoryAxis('y', ['rating', 'category']);
    chart.addMeasureAxis('x', 'percent').tickFormat = '.3p';
    chart.addSeries('category', dimple.plot.bar);
    chart.addLegend(65, 10, 510, 20, 'right');
    chart.draw();
});

d3.json('/query?query=follows_vs_submissions', function (error, j) {
    var data = j.result.map(function (v) {
        return {'user': v[0], 'submissions': v[1], 'follows': v[2]};
    });
    var svg = dimple.newSvg('#follows-vs-submissions', 800, 600);
    var chart = new dimple.chart(svg, data);
    chart.setBounds(120, 30, 660, 500)
    chart.addMeasureAxis('x', 'submissions').tickFormat = 'd';
    chart.addMeasureAxis('y', 'follows').tickFormat = 'd';
    chart.addSeries('user', dimple.plot.bubble).addEventHandler('click', function (ev) {
        window.open('https://www.weasyl.com/~' + ev.seriesValue, '_blank');
    });
    chart.draw();
});

d3.json('/query?query=favorites_vs_view_time', function (error, j) {
    var data = j.result.map(function (v) {
        return {'id': v[0], 'type': 'submission', 'average view time (s) yesterday': v[1], 'visits yesterday': v[2], 'total favorites': v[3]};
    });
    var svg = dimple.newSvg('#submission-visits-vs-view-time-vs-favorites', 800, 600);
    var chart = new dimple.chart(svg, data);
    chart.setBounds(120, 30, 660, 500)
    chart.addMeasureAxis('x', 'average view time (s) yesterday').tickFormat = 'd';
    chart.addMeasureAxis('y', 'total favorites').tickFormat = 'd';
    chart.addColorAxis('visits yesterday').tickFormat = 'd';
    chart.addSeries(['id', 'type'], dimple.plot.bubble).addEventHandler('click', function (ev) {
        var submitid = ev.seriesValue[0];
        window.open('https://www.weasyl.com/submission/' + submitid, '_blank');
    });
    chart.draw();
});

d3.json('/query?query=top_reporters', function (error, j) {
    var data = j.result.map(function (v) {
        return {'reporter': v[0], 'reports': v[1]};
    });
    var svg = dimple.newSvg('#top-reporters', 800, 600);
    var chart = new dimple.chart(svg, data);
    chart.setBounds(120, 30, 660, 500)
    chart.addMeasureAxis('x', 'reports').tickFormat = 'd';
    chart.addCategoryAxis('y', 'reporter');
    chart.addSeries(null, dimple.plot.bar);
    chart.draw();
});

// d3.json('/query?query=follows_vs_submissions_clustered', function (error, j) {
//     var data = j.result.map(function (v) {
//         return {'submissions': v[0], 'follows': v[1], 'users': Math.log(v[2])};
//     });
//     var svg = dimple.newSvg('#clustered-follows-vs-submissions', 800, 600);
//     var chart = new dimple.chart(svg, data);
//     chart.setBounds(120, 30, 660, 500)
//     chart.addMeasureAxis('x', 'submissions').tickFormat = 'd';
//     chart.addMeasureAxis('y', 'follows').tickFormat = 'd';
//     chart.addColorAxis('users');
//     chart.addSeries(null, dimple.plot.bubble);
//     chart.draw();
// });

// d3.json('/query?query=tag_counts', function (error, j) {
//     var data = j.result.map(function (v) { return [v[1], v[0]] });
//     data.sort(function (a, b) { return d3.ascending(a[0], b[0]); });
//     new Dygraph(
//         'tag-counts',
//         data,
//         {
//             labels: ['submissions', 'tags'],
//             xlabel: 'Number of submissions',
//             ylabel: 'Number of tags',
//         });
// });
