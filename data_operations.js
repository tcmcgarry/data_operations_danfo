const dfd = require("danfojs-node");

// rolling_average
// rolling_median
// discretize_column

/**
 * Calculates the moving average over a specified window of rows for each column
 * @param {DataFrame} df
 * @param {number} windowSize
 * @returns The DataFrame with rolling averages for each column
 */
async function rollingAverage(df, windowSize){
    let results = {};
    const calculateMovingAverage = (values, windowSize) => {
        let movingAverages = [];
        for (let i = 0; i < values.length; i++) {
            if (i + 1 < windowSize) {
                let windowValues = values.slice(0, i + 1);
                let sum = windowValues.reduce((a, b) => a + b, 0);
                movingAverages.push(sum / (i + 1));
            } else {
                let windowValues = values.slice(i + 1 - windowSize, i + 1);
                let sum = windowValues.reduce((a, b) => a + b, 0);
                movingAverages.push(sum / windowSize);
            }
        }
        return movingAverages;
    };
    for (let column of df.columns) {
        let columnData = df[column].values;
        if (typeof columnData[0] === 'number') {
            results[column] = calculateMovingAverage(columnData, windowSize);
        } else {
            results[column] = columnData.slice();
        }
    }
    return new dfd.DataFrame(results);
}

/**
 * Calculates the moving median over a specified window of rows for each column
 * @param {DataFrame} df
 * @returns The DataFrame with
 */
async function rollingMedian(df, windowSize){
    let results = {};
    const calculateMedian = (values) => {
        values.sort((a, b) => a - b);
        const mid = Math.floor(values.length / 2);
        return values.length % 2 !== 0 ? values[mid] : (values[mid - 1] + values[mid]) / 2.0;
    };
    for (let column of df.columns) {
        let columnData = df[column].values;
        if (typeof columnData[0] === 'number') {
            let medians = [];
            for (let i = 0; i < columnData.length; i++) {
                if (i + 1 < windowSize) {
                    medians.push(NaN);
                } else {
                    let windowValues = columnData.slice(i + 1 - windowSize, i + 1);
                    medians.push(calculateMedian(windowValues));
                }
            }
            results[column] = medians;
        } else {
            // Copy non-numeric columns as is
            results[column] = columnData.slice();
        }
    }
    return new dfd.DataFrame(results);
}

/**
 * Splits a numerical column into bins or ranges, essential for histograms
 * @param {DataFrame} df
 * @returns The DataFrame with
 */
async function discretizeColumn(df, numBins){
    const column = df.columns[0];
    let colData = df[column].values;
    let min = df[column].min();
    let max = df[column].max();
    let binWidth = (max - min) / numBins;
    const getBinIndex = (value) => {
        let adjustedValue = value === min ? value + 1e-9 : value;
        return Math.ceil((adjustedValue - min) / binWidth);
    };
    let binnedData = colData.map(value => getBinIndex(value));
    df.addColumn("Binned Data", binnedData, { inplace: true });
    return df;
}
