axios.get('http://localhost:8080/prices2')
    .then(response => {
        const responseData = response.data;

        const dates = responseData.map(obj => new Date(obj.date));
        const prices = responseData.map(obj => parseFloat(obj.average_wine_price_us));

        var plotData = [{
            x: dates,
            y: prices,
            type: 'scatter',
            mode: 'lines',
            line: {
                color: 'blue' // Change the color of the line here
            },
        }];

        var layout = {
            title: 'Average Wine Prices',
            xaxis: {
                title: 'Date',
                showgrid: true,
                zeroline: true,
                showline: true,
                titlefont: {
                    color: 'black' // Change the color of the x-axis title here
                },
                tickfont: {
                    color: 'black' // Change the color of the x-axis ticks here
                }
            },
            yaxis: {
                title: 'Price',
                showgrid: true,
                zeroline: true,
                showline: true,
                titlefont: {
                    color: 'black' // Change the color of the y-axis title here
                },
                tickfont: {
                    color: 'black' // Change the color of the y-axis ticks here
                }
            }
        };

        Plotly.newPlot('chart-container', plotData, layout);
    });
