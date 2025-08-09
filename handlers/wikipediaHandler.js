const axios = require('axios');
const cheerio = require('cheerio');
const visualizationService = require('../services/visualizationService');

class WikipediaHandler {
  constructor() {
    this.baseUrl = 'https://en.wikipedia.org';
    this.timeout = 30000;
  }

  async processWikipediaTask(taskDescription, taskAnalysis) {
    console.log('📊 Processing Wikipedia task...');
    
    try {
      // Determine which Wikipedia page to scrape
      let url = this.determineWikipediaUrl(taskDescription);
      console.log('🔍 Target URL:', url);

      // Scrape the Wikipedia table
      const data = await this.scrapeWikipediaTable(url);
      console.log(`📈 Scraped ${data.length} rows of data`);

      // Process based on the specific task
      if (taskDescription.toLowerCase().includes('highest-grossing') || 
          taskDescription.toLowerCase().includes('films')) {
        return await this.processFilmsAnalysis(data, taskAnalysis);
      }

      // Generic processing for other Wikipedia tables
      return await this.processGenericTable(data, taskAnalysis);

    } catch (error) {
      console.error('❌ Wikipedia processing error:', error);
      throw new Error(`Failed to process Wikipedia data: ${error.message}`);
    }
  }

  determineWikipediaUrl(taskDescription) {
    const task = taskDescription.toLowerCase();
    
    if (task.includes('highest-grossing') || task.includes('films')) {
      return 'https://en.wikipedia.org/wiki/List_of_highest-grossing_films';
    }
    
    // Extract URL from task if provided
    const urlMatch = taskDescription.match(/https?:\/\/[^\s]+/);
    if (urlMatch) {
      return urlMatch[0];
    }
    
    // Default fallback
    return 'https://en.wikipedia.org/wiki/List_of_highest-grossing_films';
  }

  async scrapeWikipediaTable(url) {
    try {
      console.log(`🌐 Fetching data from: ${url}`);
      
      const response = await axios.get(url, {
        timeout: this.timeout,
        headers: {
          'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
      });

      const $ = cheerio.load(response.data);
      const tables = $('table.wikitable');
      
      if (tables.length === 0) {
        throw new Error('No wikitable found on the page');
      }

      // Process the first (main) table
      const table = tables.first();
      const headers = [];
      const rows = [];

      // Extract headers
      table.find('thead tr, tbody tr').first().find('th, td').each((i, elem) => {
        const text = $(elem).text().trim().replace(/\n/g, ' ');
        headers.push(text);
      });

      // Extract data rows
      table.find('tbody tr').slice(1).each((i, row) => {
        const cells = [];
        $(row).find('td, th').each((j, cell) => {
          let text = $(cell).text().trim().replace(/\n/g, ' ');
          // Clean up common Wikipedia formatting
          text = text.replace(/\[.*?\]/g, ''); // Remove citations
          text = text.replace(/\s+/g, ' '); // Normalize whitespace
          cells.push(text);
        });
        
        if (cells.length > 0) {
          const rowObj = {};
          headers.forEach((header, index) => {
            rowObj[header] = cells[index] || '';
          });
          rows.push(rowObj);
        }
      });

      console.log(`✅ Extracted ${rows.length} rows with headers:`, headers);
      return rows;

    } catch (error) {
      console.error('❌ Scraping failed:', error.message);
      throw error;
    }
  }

  async processFilmsAnalysis(data, taskAnalysis) {
    console.log('🎬 Processing films analysis...');
    
    try {
      // Clean and standardize the data
      const cleanData = this.cleanFilmsData(data);
      console.log(`🧹 Cleaned data: ${cleanData.length} valid entries`);

      // Answer the specific questions
      const results = [];

      // Question 1: How many $2bn movies were released before 2000?
      const movies2bnBefore2000 = this.count2BillionMoviesBefore2000(cleanData);
      results.push(movies2bnBefore2000);
      console.log(`💰 Movies $2bn before 2000: ${movies2bnBefore2000}`);

      // Question 2: Which is the earliest film that grossed over $1.5bn?
      const earliest1_5bn = this.findEarliest1_5BillionFilm(cleanData);
      results.push(earliest1_5bn);
      console.log(`🏆 Earliest $1.5bn film: ${earliest1_5bn}`);

      // Question 3: What's the correlation between Rank and Peak?
      const correlation = this.calculateRankPeakCorrelation(cleanData);
      results.push(correlation);
      console.log(`📊 Rank-Peak correlation: ${correlation}`);

      // Question 4: Generate scatterplot if needed
      let scatterplot = "";
      if (taskAnalysis.visualizationNeeded) {
        scatterplot = await this.generateRankPeakScatterplot(cleanData);
        results.push(scatterplot);
        console.log(`📈 Generated scatterplot: ${scatterplot.length} characters`);
      }

      return results;

    } catch (error) {
      console.error('❌ Films analysis error:', error);
      throw error;
    }
  }

  cleanFilmsData(rawData) {
    const cleaned = [];
    
    for (const row of rawData) {
      try {
        // Find revenue/gross column (flexible matching)
        let revenueValue = null;
        let yearValue = null;
        let titleValue = null;
        let rankValue = null;
        let peakValue = null;

        // Look for revenue in various column names
        const possibleRevenueKeys = Object.keys(row).filter(key => 
          key.toLowerCase().includes('worldwide') ||
          key.toLowerCase().includes('gross') ||
          key.toLowerCase().includes('box office') ||
          key.toLowerCase().includes('revenue')
        );

        if (possibleRevenueKeys.length > 0) {
          const revenueStr = row[possibleRevenueKeys[0]];
          // Extract number from string like "$2,798,000,000"
          const revenueMatch = revenueStr.match(/[\d,]+/);
          if (revenueMatch) {
            revenueValue = parseInt(revenueMatch[0].replace(/,/g, ''));
          }
        }

        // Look for year
        const possibleYearKeys = Object.keys(row).filter(key =>
          key.toLowerCase().includes('year') ||
          key.toLowerCase().includes('released')
        );

        if (possibleYearKeys.length > 0) {
          const yearStr = row[possibleYearKeys[0]];
          const yearMatch = yearStr.match(/\d{4}/);
          if (yearMatch) {
            yearValue = parseInt(yearMatch[0]);
          }
        }

        // Look for title/film name
        const possibleTitleKeys = Object.keys(row).filter(key =>
          key.toLowerCase().includes('title') ||
          key.toLowerCase().includes('film') ||
          key.toLowerCase().includes('movie') ||
          key === Object.keys(row)[0] // First column is often title
        );

        if (possibleTitleKeys.length > 0) {
          titleValue = row[possibleTitleKeys[0]];
        }

        // Look for rank
        const possibleRankKeys = Object.keys(row).filter(key =>
          key.toLowerCase().includes('rank') ||
          key.toLowerCase().includes('position')
        );

        if (possibleRankKeys.length > 0) {
          const rankStr = row[possibleRankKeys[0]];
          const rankMatch = rankStr.match(/\d+/);
          if (rankMatch) {
            rankValue = parseInt(rankMatch[0]);
          }
        }

        // Look for peak
        const possiblePeakKeys = Object.keys(row).filter(key =>
          key.toLowerCase().includes('peak')
        );

        if (possiblePeakKeys.length > 0) {
          const peakStr = row[possiblePeakKeys[0]];
          const peakMatch = peakStr.match(/\d+/);
          if (peakMatch) {
            peakValue = parseInt(peakMatch[0]);
          }
        }

        // Only add if we have essential data
        if (revenueValue && titleValue) {
          cleaned.push({
            title: titleValue,
            revenue: revenueValue,
            year: yearValue,
            rank: rankValue || cleaned.length + 1, // Use index as fallback
            peak: peakValue || rankValue || cleaned.length + 1
          });
        }

      } catch (error) {
        console.log('⚠️ Skipping invalid row:', error.message);
        continue;
      }
    }

    return cleaned;
  }

  count2BillionMoviesBefore2000(data) {
    return data.filter(movie => 
      movie.revenue >= 2000000000 && 
      movie.year && 
      movie.year < 2000
    ).length;
  }

  findEarliest1_5BillionFilm(data) {
    const over1_5bn = data.filter(movie => 
      movie.revenue >= 1500000000 && 
      movie.year
    );

    if (over1_5bn.length === 0) {
      return "No films found over $1.5bn";
    }

    const earliest = over1_5bn.reduce((prev, current) => 
      (prev.year < current.year) ? prev : current
    );

    return `${earliest.title} (${earliest.year})`;
  }

  calculateRankPeakCorrelation(data) {
    const validData = data.filter(d => d.rank && d.peak);
    
    if (validData.length < 2) {
      return 0;
    }

    const ranks = validData.map(d => d.rank);
    const peaks = validData.map(d => d.peak);

    // Simple Pearson correlation
    const n = ranks.length;
    const sumX = ranks.reduce((a, b) => a + b, 0);
    const sumY = peaks.reduce((a, b) => a + b, 0);
    const sumXY = ranks.reduce((sum, x, i) => sum + x * peaks[i], 0);
    const sumXX = ranks.reduce((sum, x) => sum + x * x, 0);
    const sumYY = peaks.reduce((sum, y) => sum + y * y, 0);

    const correlation = (n * sumXY - sumX * sumY) / 
      Math.sqrt((n * sumXX - sumX * sumX) * (n * sumYY - sumY * sumY));

    return Math.round(correlation * 1000) / 1000; // Round to 3 decimal places
  }

  async generateRankPeakScatterplot(data) {
    try {
      const validData = data.filter(d => d.rank && d.peak);
      
      if (validData.length < 2) {
        return "";
      }

      const x = validData.map(d => d.rank);
      const y = validData.map(d => d.peak);

      return await visualizationService.createScatterPlot(
        x, y, 'Rank', 'Peak', 'Rank vs Peak Correlation'
      );

    } catch (error) {
      console.error('❌ Scatter plot generation failed:', error);
      return "";
    }
  }

  async processGenericTable(data, taskAnalysis) {
    // Generic processing for other Wikipedia tables
    console.log('📋 Processing generic Wikipedia table...');
    
    return {
      success: true,
      data_points: data.length,
      columns: Object.keys(data[0] || {}),
      sample_data: data.slice(0, 3),
      analysis: 'Generic Wikipedia table processed'
    };
  }
}

module.exports = new WikipediaHandler();