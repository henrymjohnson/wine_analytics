Promise.all([
    axios.get('http://localhost:8080/demographics?type=population'),
    axios.get('http://localhost:8080/demographics?type=disposable_income')
]).then(([response1, response2]) => {
    const responseData1 = response1.data;
    const responseData2 = response2.data;

    const valueName1 = 'Population';
    const valueName2 = 'Disposable Income';
    const dates1 = responseData1.map(obj => new Date(obj.date));
    const values1 = responseData1.map(obj => obj.population_size);
    const dates2 = responseData2.map(obj => new Date(obj.date));
    const values2 = responseData2.map(obj => obj.disposable_income_amount);
    
    var plotData1 = [{
        x: dates1,
        y: values1,
        type: 'scatter',
        mode: 'lines',
        name: valueName1,
        line: {
            color: 'orange'
        }
    }];
    
    var plotData2 = [{
        x: dates2,
        y: values2,
        type: 'scatter',
        mode: 'lines',
        name: valueName2,
        line: {
            color: 'red'
        }
    }];
    
    var layout = {
        title: 'Demographics Data',
        xaxis: {
            title: 'Date',
            showgrid: true,
            zeroline: true,
            showline: true,
            titlefont: {
                color: 'black'
            },
            tickfont: {
                color: 'black'
            }
        },
        yaxis: {
            title: 'Value',
            showgrid: true,
            zeroline: true,
            showline: true,
            titlefont: {
                color: 'black'
            },
            tickfont: {
                color: 'black'
            }
        }
    };

    Plotly.newPlot('chart-container', plotData1.concat(plotData2), layout);
});
