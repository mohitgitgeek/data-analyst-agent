const csv = require('csv-parser');
const { Readable } = require('stream');
const visualizationService = require('../services/visualizationService');

class CSVHandler {
  constructor() {
    this.maxRows = 10000; // Limit for performance
  }

  async processCSVTask(taskDescription, csvData, taskAnalysis) {
    console.log('📁 Processing CSV task...');
    
    try {
      if (!csvData) {
        throw new Error('No CSV data provided');
      }

      // Parse CSV data
      const parsedData = await this.parseCSV(csvData);
      console.log(`📊 Parsed ${parsedData.length} rows of CSV data`);

      // Analyze the data structure
      const analysis = this.analyzeCSVStructure(parsedData);
      console.log('🔍 CSV Analysis:', analysis);

      // Process based on task requirements
      const results = await this.processAnalysis(parsedData, taskDescription, taskAnalysis);

      return {
        success: true,
        data_summary: analysis,
        results: results,
        rows_processed: parsedData.length
      };

    } catch (error) {
      console.error('❌ CSV processing error:', error);
      throw error;
    }
  }

  async parseCSV(csvData) {
    return new Promise((resolve, reject) => {
      const results = [];
      const stream = Readable.from([csvData]);
      
      stream
        .pipe(csv())
        .on('data', (data) => {
          // Limit rows for performance
          if (results.length < this.maxRows) {
            results.push(data);
          }
        })
        .on('end', () => {
          console.log(`✅ CSV parsing complete: ${results.length} rows`);
          resolve(results);
        })
        .on('error', (error) => {
          console.error('❌ CSV parsing failed:', error);
          reject(error);
        });
    });
  }

  analyzeCSVStructure(data) {
    if (!data || data.length === 0) {
      return { error: 'No data to analyze' };
    }

    const columns = Object.keys(data[0]);
    const analysis = {
      total_rows: data.length,
      total_columns: columns.length,
      columns: columns,
      column_types: {},
      missing_values: {},
      sample_data: data.slice(0, 3)
    };

    // Analyze each column
    columns.forEach(col => {
      const values = data.map(row => row[col]).filter(val => val !== '' && val != null);
      const nonEmptyCount = values.length;
      const missingCount = data.length - nonEmptyCount;
      
      analysis.missing_values[col] = missingCount;
      
      // Determine column type
      const numericValues = values.filter(val => !isNaN(Number(val)));
      
      if (numericValues.length > values.length * 0.8) {
        analysis.column_types[col] = 'numeric';
      } else {
        analysis.column_types[col] = 'text';
      }
    });

    return analysis;
  }

  async processAnalysis(data, taskDescription, taskAnalysis) {
    const results = {};
    
    try {
      // Basic statistical analysis
      results.basic_statistics = this.calculateBasicStats(data);
      
      // Correlation analysis if requested
      if (taskAnalysis.analysisType === 'correlation' || 
          taskDescription.toLowerCase().includes('correlation')) {
        results.correlations = this.calculateCorrelations(data);
      }

      // Generate visualizations if needed
      if (taskAnalysis.visualizationNeeded) {
        results.visualizations = await this.generateVisualizations(data);
      }

      return results;

    } catch (error) {
      console.error('❌ Analysis processing failed:', error);
      return { error: error.message };
    }
  }

  calculateBasicStats(data) {
    if (!data || data.length === 0) return {};

    const stats = {};
    const columns = Object.keys(data[0]);

    columns.forEach(col => {
      const values = data
        .map(row => row[col])
        .filter(val => val !== '' && val != null)
        .map(val => Number(val))
        .filter(val => !isNaN(val));

      if (values.length > 0) {
        values.sort((a, b) => a - b);
        
        stats[col] = {
          count: values.length,
          mean: values.reduce((a, b) => a + b, 0) / values.length,
          median: values[Math.floor(values.length / 2)],
          min: Math.min(...values),
          max: Math.max(...values),
          std_dev: this.calculateStdDev(values)
        };
      }
    });

    return stats;
  }

  calculateStdDev(values) {
    const mean = values.reduce((a, b) => a + b, 0) / values.length;
    const squareDiffs = values.map(value => Math.pow(value - mean, 2));
    const avgSquareDiff = squareDiffs.reduce((a, b) => a + b, 0) / values.length;
    return Math.sqrt(avgSquareDiff);
  }

  calculateCorrelations(data) {
    if (!data || data.length < 2) return {};

    const correlations = {};
    const columns = Object.keys(data[0]);
    const numericColumns = columns.filter(col => {
      const values = data.map(row => Number(row[col])).filter(val => !isNaN(val));
      return values.length > data.length * 0.5; // At least 50% numeric
    });

    // Calculate correlations between numeric columns
    for (let i = 0; i < numericColumns.length; i++) {
      for (let j = i + 1; j < numericColumns.length; j++) {
        const col1 = numericColumns[i];
        const col2 = numericColumns[j];
        
        const values1 = data.map(row => Number(row[col1])).filter(val => !isNaN(val));
        const values2 = data.map(row => Number(row[col2])).filter(val => !isNaN(val));
        
        if (values1.length === values2.length && values1.length > 1) {
          const correlation = this.pearsonCorrelation(values1, values2);
          correlations[`${col1}_vs_${col2}`] = Math.round(correlation * 1000) / 1000;
        }
      }
    }

    return correlations;
  }

  pearsonCorrelation(x, y) {
    const n = x.length;
    const sumX = x.reduce((a, b) => a + b, 0);
    const sumY = y.reduce((a, b) => a + b, 0);
    const sumXY = x.reduce((sum, xi, i) => sum + xi * y[i], 0);
    const sumXX = x.reduce((sum, xi) => sum + xi * xi, 0);
    const sumYY = y.reduce((sum, yi) => sum + yi * yi, 0);
    
    const correlation = (n * sumXY - sumX * sumY) / 
      Math.sqrt((n * sumXX - sumX * sumX) * (n * sumYY - sumY * sumY));
    
    return isNaN(correlation) ? 0 : correlation;
  }

  async generateVisualizations(data) {
    const visualizations = {};
    
    try {
      const columns = Object.keys(data[0]);
      const numericColumns = columns.filter(col => {
        const values = data.map(row => Number(row[col])).filter(val => !isNaN(val));
        return values.length > data.length * 0.5;
      });

      // Create scatter plot for first two numeric columns
      if (numericColumns.length >= 2) {
        const col1 = numericColumns[0];
        const col2 = numericColumns[1];
        
        const values1 = data.map(row => Number(row[col1])).filter(val => !isNaN(val));
        const values2 = data.map(row => Number(row[col2])).filter(val => !isNaN(val));
        
        if (values1.length > 1 && values2.length > 1) {
          const scatterPlot = await visualizationService.createScatterPlot(
            values1, values2, col1, col2, `${col1} vs ${col2}`
          );
          
          if (scatterPlot) {
            visualizations.scatter_plot = scatterPlot;
          }
        }
      }

      return visualizations;

    } catch (error) {
      console.error('❌ Visualization generation failed:', error);
      return {};
    }
  }
}

module.exports = new CSVHandler();

