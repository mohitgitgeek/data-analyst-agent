const { ChartJSNodeCanvas } = require('chartjs-node-canvas');
const { Chart, registerables } = require('chart.js');

// Register Chart.js components
Chart.register(...registerables);

class VisualizationService {
  constructor() {
    this.width = 800;
    this.height = 600;
    this.maxImageSize = 100000; // 100KB limit
    
    // Create chart renderer
    this.chartJSNodeCanvas = new ChartJSNodeCanvas({
      width: this.width,
      height: this.height,
      backgroundColour: 'white',
      chartCallback: (ChartJS) => {
        ChartJS.defaults.responsive = false;
        ChartJS.defaults.maintainAspectRatio = false;
      }
    });
  }

  async createScatterPlot(xData, yData, xLabel, yLabel, title) {
    try {
      console.log(`📈 Creating scatter plot: ${title}`);
      console.log(`📊 Data points: ${xData.length}`);

      // Validate input data
      if (!Array.isArray(xData) || !Array.isArray(yData)) {
        throw new Error('Input data must be arrays');
      }

      if (xData.length !== yData.length) {
        throw new Error('X and Y data arrays must have the same length');
      }

      if (xData.length < 2) {
        throw new Error('Need at least 2 data points for scatter plot');
      }

      // Clean data - remove null/undefined values
      const cleanData = [];
      for (let i = 0; i < xData.length; i++) {
        if (xData[i] != null && yData[i] != null && 
            !isNaN(xData[i]) && !isNaN(yData[i])) {
          cleanData.push({ x: Number(xData[i]), y: Number(yData[i]) });
        }
      }

      if (cleanData.length < 2) {
        throw new Error('Insufficient valid data points after cleaning');
      }

      console.log(`✅ Using ${cleanData.length} clean data points`);

      // Calculate regression line
      const regression = this.calculateRegression(cleanData);
      const regressionLine = this.generateRegressionPoints(cleanData, regression);

      // Create chart configuration
      const configuration = {
        type: 'scatter',
        data: {
          datasets: [
            {
              label: 'Data Points',
              data: cleanData,
              backgroundColor: 'rgba(54, 162, 235, 0.6)',
              borderColor: 'rgba(54, 162, 235, 1)',
              borderWidth: 1,
              pointRadius: 4,
              showLine: false
            },
            {
              label: `Regression Line (slope: ${regression.slope.toFixed(3)})`,
              data: regressionLine,
              backgroundColor: 'rgba(255, 99, 132, 0)',
              borderColor: 'rgba(255, 99, 132, 1)',
              borderWidth: 2,
              borderDash: [5, 5], // Dotted line
              pointRadius: 0,
              showLine: true,
              fill: false
            }
          ]
        },
        options: {
          responsive: false,
          maintainAspectRatio: false,
          plugins: {
            title: {
              display: true,
              text: title,
              font: {
                size: 16,
                weight: 'bold'
              }
            },
            legend: {
              display: true,
              position: 'top'
            }
          },
          scales: {
            x: {
              type: 'linear',
              position: 'bottom',
              title: {
                display: true,
                text: xLabel,
                font: {
                  size: 14,
                  weight: 'bold'
                }
              },
              grid: {
                display: true,
                color: 'rgba(0, 0, 0, 0.1)'
              }
            },
            y: {
              title: {
                display: true,
                text: yLabel,
                font: {
                  size: 14,
                  weight: 'bold'
                }
              },
              grid: {
                display: true,
                color: 'rgba(0, 0, 0, 0.1)'
              }
            }
          }
        }
      };

      // Generate the chart image
      let imageBuffer = await this.chartJSNodeCanvas.renderToBuffer(configuration);
      
      // Check size and reduce quality if necessary
      if (imageBuffer.length > this.maxImageSize) {
        console.log(`⚠️ Image too large (${imageBuffer.length} bytes), reducing size...`);
        
        // Create smaller chart
        const smallCanvas = new ChartJSNodeCanvas({
          width: 600,
          height: 400,
          backgroundColour: 'white'
        });
        
        imageBuffer = await smallCanvas.renderToBuffer(configuration);
        
        if (imageBuffer.length > this.maxImageSize) {
          console.log(`⚠️ Still too large, creating minimal chart...`);
          
          // Create minimal chart
          const minimalCanvas = new ChartJSNodeCanvas({
            width: 400,
            height: 300,
            backgroundColour: 'white'
          });
          
          imageBuffer = await minimalCanvas.renderToBuffer(configuration);
        }
      }

      // Convert to base64
      const base64Image = imageBuffer.toString('base64');
      const dataUri = `data:image/png;base64,${base64Image}`;

      console.log(`✅ Generated scatter plot: ${imageBuffer.length} bytes`);
      return dataUri;

    } catch (error) {
      console.error('❌ Scatter plot generation failed:', error);
      return "";
    }
  }

  calculateRegression(data) {
    const n = data.length;
    const sumX = data.reduce((sum, point) => sum + point.x, 0);
    const sumY = data.reduce((sum, point) => sum + point.y, 0);
    const sumXY = data.reduce((sum, point) => sum + (point.x * point.y), 0);
    const sumXX = data.reduce((sum, point) => sum + (point.x * point.x), 0);

    const slope = (n * sumXY - sumX * sumY) / (n * sumXX - sumX * sumX);
    const intercept = (sumY - slope * sumX) / n;

    return { slope, intercept };
  }

  generateRegressionPoints(data, regression) {
    const xValues = data.map(point => point.x);
    const minX = Math.min(...xValues);
    const maxX = Math.max(...xValues);

    return [
      { x: minX, y: regression.slope * minX + regression.intercept },
      { x: maxX, y: regression.slope * maxX + regression.intercept }
    ];
  }

  async createBarChart(labels, data, title, xLabel, yLabel) {
    try {
      console.log(`📊 Creating bar chart: ${title}`);

      const configuration = {
        type: 'bar',
        data: {
          labels: labels,
          datasets: [{
            label: title,
            data: data,
            backgroundColor: 'rgba(54, 162, 235, 0.6)',
            borderColor: 'rgba(54, 162, 235, 1)',
            borderWidth: 1
          }]
        },
        options: {
          responsive: false,
          maintainAspectRatio: false,
          plugins: {
            title: {
              display: true,
              text: title,
              font: {
                size: 16,
                weight: 'bold'
              }
            }
          },
          scales: {
            x: {
              title: {
                display: true,
                text: xLabel
              }
            },
            y: {
              title: {
                display: true,
                text: yLabel
              }
            }
          }
        }
      };

      const imageBuffer = await this.chartJSNodeCanvas.renderToBuffer(configuration);
      const base64Image = imageBuffer.toString('base64');
      const dataUri = `data:image/png;base64,${base64Image}`;

      console.log(`✅ Generated bar chart: ${imageBuffer.length} bytes`);
      return dataUri;

    } catch (error) {
      console.error('❌ Bar chart generation failed:', error);
      return "";
    }
  }

  async createLineChart(xData, yData, title, xLabel, yLabel) {
    try {
      console.log(`📈 Creating line chart: ${title}`);

      const chartData = xData.map((x, i) => ({ x, y: yData[i] }));

      const configuration = {
        type: 'line',
        data: {
          datasets: [{
            label: title,
            data: chartData,
            backgroundColor: 'rgba(75, 192, 192, 0.2)',
            borderColor: 'rgba(75, 192, 192, 1)',
            borderWidth: 2,
            fill: false
          }]
        },
        options: {
          responsive: false,
          maintainAspectRatio: false,
          plugins: {
            title: {
              display: true,
              text: title,
              font: {
                size: 16,
                weight: 'bold'
              }
            }
          },
          scales: {
            x: {
              type: 'linear',
              title: {
                display: true,
                text: xLabel
              }
            },
            y: {
              title: {
                display: true,
                text: yLabel
              }
            }
          }
        }
      };

      const imageBuffer = await this.chartJSNodeCanvas.renderToBuffer(configuration);
      const base64Image = imageBuffer.toString('base64');
      const dataUri = `data:image/png;base64,${base64Image}`;

      console.log(`✅ Generated line chart: ${imageBuffer.length} bytes`);
      return dataUri;

    } catch (error) {
      console.error('❌ Line chart generation failed:', error);
      return "";
    }
  }

  async createCorrelationMatrix(data, labels) {
    try {
      console.log('🔥 Creating correlation matrix heatmap');
      
      // For now, create a simple representation
      // In a full implementation, you'd create an actual heatmap
      const correlations = this.calculateCorrelationMatrix(data);
      
      return this.createBarChart(
        labels,
        correlations[0], // First row of correlations
        'Correlation Matrix',
        'Variables',
        'Correlation'
      );

    } catch (error) {
      console.error('❌ Correlation matrix generation failed:', error);
      return "";
    }
  }

  calculateCorrelationMatrix(data) {
    // Simple correlation matrix calculation
    const matrix = [];
    
    for (let i = 0; i < data.length; i++) {
      const row = [];
      for (let j = 0; j < data.length; j++) {
        if (i === j) {
          row.push(1);
        } else {
          row.push(this.pearsonCorrelation(data[i], data[j]));
        }
      }
      matrix.push(row);
    }
    
    return matrix;
  }

  pearsonCorrelation(x, y) {
    const n = x.length;
    if (n !== y.length) return 0;
    
    const sumX = x.reduce((a, b) => a + b, 0);
    const sumY = y.reduce((a, b) => a + b, 0);
    const sumXY = x.reduce((sum, xi, i) => sum + xi * y[i], 0);
    const sumXX = x.reduce((sum, xi) => sum + xi * xi, 0);
    const sumYY = y.reduce((sum, yi) => sum + yi * yi, 0);
    
    const correlation = (n * sumXY - sumX * sumY) / 
      Math.sqrt((n * sumXX - sumX * sumX) * (n * sumYY - sumY * sumY));
    
    return isNaN(correlation) ? 0 : correlation;
  }
}

module.exports = new VisualizationService();