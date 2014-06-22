var dateFormat = d3.time.format('%Y-%m-%dT%H:%M:%S');

d3.json('/query?query=tickets', function (error, j) {
    var nest = d3.nest()
	.key(function (d) { return d.reporter; })
	.sortValues(function (a, b) { return d3.ascending(a.openedAt, b.openedAt); })
	.entries(j.result.map(function (v) {
	    return {reporter: v[0], openedAt: dateFormat.parse(v[1]).getTime(), count: v[2]};
	}));
    var times = [];
    nest.forEach(function (v1) {
	v1.values.forEach(function (v2) {
	    if (times.indexOf(v2.openedAt) === -1) {
		times.push(v2.openedAt);
	    }
	});
    });
    times.sort(d3.ascending);
    var countsByCloser = [];
    var data = nest.map(function (v1) {
	var monthMap = d3.map();
	var total = 0;
	v1.values.forEach(function (v2) {
	    monthMap.set(v2.openedAt, v2.count);
	    total += v2.count;
	});
	countsByCloser.push({
	    closer: v1.key,
	    count: total,
	});
	return {
	    key: v1.key,
	    values: times.map(function (month) {
		return {x: new Date(parseInt(month)), y: monthMap.get(month) || 0};
	    }),
	};
    });
    data.sort(function (a, b) { return d3.descending(a.key, b.key); });

    nv.addGraph(function () {
	var chart = nv.models.stackedAreaChart()
	    .useInteractiveGuideline(true)
	    .style('stream');

	chart.xAxis.tickFormat(function(d) {
	    return d3.time.format('%Y-%m')(new Date(d));
	});

	chart.yAxis.tickFormat(d3.format(',.0f'));

	d3.select('#chart')
	    .datum(data)
	    .call(chart);

	return chart;
    });

    var svg = dimple.newSvg('#closers', 800, 600);
    var chart = new dimple.chart(svg, countsByCloser);
    chart.setBounds(120, 30, 660, 500)
    var axisX = chart.addMeasureAxis('x', 'count');
    axisX.tickFormat = 'd';
    chart.addCategoryAxis('y', 'closer');
    chart.addSeries(null, dimple.plot.bar);
    chart.draw();
});

var dateFormat = d3.time.format('%Y-%m-%dT%H:%M:%S');

d3.json('/query?query=reports_by_day', function (error, j) {
    var data = j.result.map(function (v) {
        return [dateFormat.parse(v[0]), v[1]];
    });
    data.sort(function (a, b) { return d3.ascending(a[0], b[0]); });
    new Dygraph(
        'reports-by-day',
        data,
        {
            labels: ['date', 'reports'],
            xlabel: 'Report date',
            ylabel: 'Number of reports',
        });
});
