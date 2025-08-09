const { OpenAI } = require('openai');

class LLMService {
  constructor() {
    if (!process.env.OPENAI_API_KEY) {
      console.warn('OpenAI API key not found. LLM features will be limited.');
      this.openai = null;
    } else {
      this.openai = new OpenAI({
        apiKey: process.env.OPENAI_API_KEY
      });
    }
  }

  async analyzeTask(taskDescription) {
    try {
      if (!this.openai) {
        // Fallback analysis without LLM
        return this.fallbackAnalysis(taskDescription);
      }

      const prompt = `
Analyze this data analysis task and return a JSON object with the following structure:
{
  "requiresWikipedia": boolean,
  "requiresCourtData": boolean, 
  "requiresCSV": boolean,
  "dataSource": "wikipedia|court_data|csv|unknown",
  "analysisType": "correlation|regression|count|visualization|statistical_summary",
  "expectedOutputFormat": "json_array|json_object|base64_image",
  "questions": ["question1", "question2"],
  "visualizationNeeded": boolean,
  "statisticalOperations": ["correlation", "regression", "count", "filter"]
}

Task description: "${taskDescription}"

Examples:
- If task mentions "highest-grossing films" or "Wikipedia" → requiresWikipedia: true
- If task mentions "Indian high court" or "court data" → requiresCourtData: true  
- If task asks for correlation or scatterplot → analysisType: "correlation", visualizationNeeded: true
- If task asks for counts or "how many" → analysisType: "count"

Return only valid JSON, no explanation.`;

      const response = await this.openai.chat.completions.create({
        model: "gpt-3.5-turbo",
        messages: [
          {
            role: "system", 
            content: "You are a data analysis task parser. Return only valid JSON."
          },
          {
            role: "user",
            content: prompt
          }
        ],
        temperature: 0.1,
        max_tokens: 500
      });

      const content = response.choices[0].message.content.trim();
      console.log('LLM Response:', content);
      
      // Clean up response and parse JSON
      const cleanedContent = content.replace(/```json\n?|\n?```/g, '').trim();
      const analysis = JSON.parse(cleanedContent);
      
      return analysis;

    } catch (error) {
      console.error('LLM Analysis Error:', error);
      return this.fallbackAnalysis(taskDescription);
    }
  }

  fallbackAnalysis(taskDescription) {
    const task = taskDescription.toLowerCase();
    
    // Pattern matching for task analysis
    const analysis = {
      requiresWikipedia: task.includes('wikipedia') || task.includes('highest-grossing') || task.includes('films'),
      requiresCourtData: task.includes('court') || task.includes('judgment') || task.includes('indian high court'),
      requiresCSV: task.includes('csv') || task.includes('upload'),
      dataSource: 'unknown',
      analysisType: 'statistical_summary',
      expectedOutputFormat: 'json_array',
      questions: [],
      visualizationNeeded: task.includes('plot') || task.includes('chart') || task.includes('scatter') || task.includes('graph'),
      statisticalOperations: []
    };

    // Determine data source
    if (analysis.requiresWikipedia) analysis.dataSource = 'wikipedia';
    else if (analysis.requiresCourtData) analysis.dataSource = 'court_data';  
    else if (analysis.requiresCSV) analysis.dataSource = 'csv';

    // Determine analysis type
    if (task.includes('correlation')) {
      analysis.analysisType = 'correlation';
      analysis.statisticalOperations.push('correlation');
    }
    if (task.includes('regression')) {
      analysis.analysisType = 'regression';
      analysis.statisticalOperations.push('regression');
    }
    if (task.includes('how many') || task.includes('count')) {
      analysis.analysisType = 'count';
      analysis.statisticalOperations.push('count');
    }

    // Check for visualization needs
    if (task.includes('base64') || task.includes('data:image')) {
      analysis.expectedOutputFormat = 'base64_image';
      analysis.visualizationNeeded = true;
    }

    console.log('Fallback analysis:', analysis);
    return analysis;
  }

  async interpretQuestions(taskDescription) {
    try {
      if (!this.openai) {
        return this.extractQuestionsFromText(taskDescription);
      }

      const prompt = `
Extract the specific questions from this data analysis task. Return as a JSON array of strings.

Task: "${taskDescription}"

Examples:
- "How many $2 bn movies were released before 2000?" 
- "Which is the earliest film that grossed over $1.5 bn?"
- "What's the correlation between the Rank and Peak?"

Return only the JSON array, no explanation.`;

      const response = await this.openai.chat.completions.create({
        model: "gpt-3.5-turbo",
        messages: [{ role: "user", content: prompt }],
        temperature: 0.1,
        max_tokens: 300
      });

      const content = response.choices[0].message.content.trim();
      const questions = JSON.parse(content);
      return Array.isArray(questions) ? questions : [];

    } catch (error) {
      console.error('Question extraction error:', error);
      return this.extractQuestionsFromText(taskDescription);
    }
  }

  extractQuestionsFromText(text) {
    // Simple regex to find questions
    const questionPattern = /[0-9]\.\s*([^?]+\?)/g;
    const questions = [];
    let match;
    
    while ((match = questionPattern.exec(text)) !== null) {
      questions.push(match[1].trim());
    }
    
    return questions;
  }
}

module.exports = new LLMService();