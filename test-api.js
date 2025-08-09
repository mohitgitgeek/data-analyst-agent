const axios = require('axios');
const fs = require('fs');

const API_BASE = 'http://localhost:8000';

// Test samples matching the project requirements
const testCases = [
  {
    name: 'Wikipedia Films Analysis',
    description: 'Test the films analysis endpoint',
    data: `Scrape the list of highest grossing films from Wikipedia. It is at the URL:
https://en.wikipedia.org/wiki/List_of_highest-grossing_films

Answer the following questions and respond with a JSON array of strings containing the answer.

1. How many $2 bn movies were released before 2000?
2. Which is the earliest film that grossed over $1.5 bn?
3. What's the correlation between the Rank and Peak?
4. Draw a scatterplot of Rank and Peak along with a dotted red regression line through it.
   Return as a base-64 encoded data URI, "data:image/png;base64,iVBORw0KG..." under 100,000 bytes.`
  },
  {
    name: 'Court Data Analysis',
    description: 'Test the court data analysis',
    data: `The Indian high court judgement dataset contains judgements from the Indian High Courts, downloaded from ecourts website. It contains judgments of 25 high courts, along with raw metadata (as .json) and structured metadata (as .parquet).

Answer the following questions and respond with a JSON object containing the answer.

{
  "Which high court disposed the most cases from 2019 - 2022?": "...",
  "What's the regression slope of the date_of_registration - decision_date by year in the court=33_10?": "...",
  "Plot the year and # of days of delay from the above question as a scatterplot with a regression line. Encode as a base64 data URI under 100,000 characters": "data:image/webp:base64,..."
}`
  },
  {
    name: 'CSV Analysis',
    description: 'Test CSV upload and analysis',
    csvData: `Name,Age,Salary,Department,Experience
John Smith,25,50000,Engineering,2
Jane Doe,30,60000,Marketing,5
Bob Johnson,35,75000,Engineering,8
Alice Brown,28,55000,Sales,3
Charlie Davis,32,70000,Marketing,6
Eva Wilson,27,52000,Engineering,4
Frank Miller,29,58000,Sales,5
Grace Lee,31,65000,Marketing,7`,
    data: 'Analyze the uploaded CSV data and provide statistical insights, correlations, and visualizations.'
  }
];

async function testHealthEndpoint() {
  try {
    console.log('🏥 Testing health endpoint...');
    const response = await axios.get(`${API_BASE}/health`);
    console.log('✅ Health check passed:', response.data);
    return true;
  } catch (error) {
    console.error('❌ Health check failed:', error.message);
    return false;
  }
}

async function testTextRequest(testCase) {
  try {
    console.log(`\n🧪 Testing: ${testCase.name}`);
    console.log(`📝 Description: ${testCase.description}`);
    
    const startTime = Date.now();
    
    const response = await axios.post(`${API_BASE}/api/`, testCase.data, {
      headers: {
        'Content-Type': 'text/plain'
      },
      timeout: 180000 // 3 minutes
    });
    
    const endTime = Date.now();
    const duration = (endTime - startTime) / 1000;
    
    console.log(`⏱️ Duration: ${duration}s`);
    console.log('✅ Response received');
    console.log('📊 Response data:', JSON.stringify(response.data, null, 2));
    
    return { success: true, duration, data: response.data };
    
  } catch (error) {
    console.error(`❌ Test failed: ${testCase.name}`);
    console.error('Error:', error.response?.data || error.message);
    return { success: false, error: error.message };
  }
}

async function testCSVUpload(testCase) {
  try {
    console.log(`\n🧪 Testing: ${testCase.name}`);
    console.log(`📝 Description: ${testCase.description}`);
    
    const FormData = require('form-data');
    const form = new FormData();
    
    // Create a buffer from CSV data
    const csvBuffer = Buffer.from(testCase.csvData, 'utf-8');
    form.append('file', csvBuffer, {
      filename: 'test.csv',
      contentType: 'text/csv'
    });
    form.append('task', testCase.data);
    
    const startTime = Date.now();
    
    const response = await axios.post(`${API_BASE}/api/`, form, {
      headers: {
        ...form.getHeaders()
      },
      timeout: 180000 // 3 minutes
    });
    
    const endTime = Date.now();
    const duration = (endTime - startTime) / 1000;
    
    console.log(`⏱️ Duration: ${duration}s`);
    console.log('✅ CSV upload test passed');
    console.log('📊 Response data:', JSON.stringify(response.data, null, 2));
    
    return { success: true, duration, data: response.data };
    
  } catch (error) {
    console.error(`❌ CSV upload test failed: ${testCase.name}`);
    console.error('Error:', error.response?.data || error.message);
    return { success: false, error: error.message };
  }
}

async function runAllTests() {
  console.log('🚀 Starting API tests...');
  console.log('=' * 50);
  
  // Test health endpoint first
  const healthOk = await testHealthEndpoint();
  if (!healthOk) {
    console.log('❌ Server not ready. Make sure the server is running on port 8000');
    return;
  }
  
  const results = [];
  
  // Test text-based requests
  for (const testCase of testCases) {
    if (testCase.csvData) {
      // This is a CSV test
      const result = await testCSVUpload(testCase);
      results.push({ testCase: testCase.name, ...result });
    } else {
      // This is a text test
      const result = await testTextRequest(testCase);
      results.push({ testCase: testCase.name, ...result });
    }
    
    // Wait between tests
    await new Promise(resolve => setTimeout(resolve, 2000));
  }
  
  // Summary
  console.log('\n📋 TEST SUMMARY');
  console.log('=' * 50);
  
  const passed = results.filter(r => r.success).length;
  const total = results.length;
  
  console.log(`✅ Passed: ${passed}/${total}`);
  console.log(`❌ Failed: ${total - passed}/${total}`);
  
  results.forEach(result => {
    const status = result.success ? '✅' : '❌';
    const duration = result.duration ? `(${result.duration}s)` : '';
    console.log(`${status} ${result.testCase} ${duration}`);
  });
  
  if (passed === total) {
    console.log('\n🎉 All tests passed! Your API is ready for deployment.');
  } else {
    console.log('\n⚠️ Some tests failed. Check the errors above.');
  }
}

// Run tests if called directly
if (require.main === module) {
  runAllTests().catch(console.error);
}

module.exports = { runAllTests, testHealthEndpoint, testTextRequest, testCSVUpload };