const express = require('express');
const moment = require('moment');
const { createClient } = require('@supabase/supabase-js');
require('dotenv').config();

const app = express();
const PORT = process.env.PORT || 3000;

// Middleware
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

// Supabase client initialization
const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_ANON_KEY
);

// Utility functions
function logWithTimestamp(message, level = 'INFO') {
  const timestamp = moment().format('YYYY-MM-DD HH:mm:ss');
  console.log(`[${timestamp}] [${level}] ${message}`);
}

// ========================================
// MODULE 1: SMG DAILY DATES CALCULATION
// ========================================

/**
 * Calculate last N business days (excluding weekends and holidays)
 * Used for determining which dates to process in the SMG pipeline
 */
async function calculateBusinessDays(daysBack = 3) {
  try {
    logWithTimestamp(`Calculating last ${daysBack} business days`);
    
    const businessDays = [];
    let currentDate = moment().subtract(1, 'day'); // Start from yesterday
    let daysFound = 0;
    
    // Get holidays from calendar table
    const { data: holidays, error: holidayError } = await supabase
      .from('calendar')
      .select('date')
      .eq('is_holiday', true);
    
    if (holidayError) {
      logWithTimestamp(`Warning: Could not fetch holidays: ${holidayError.message}`, 'WARN');
    }
    
    const holidayDates = holidays ? holidays.map(h => h.date) : [];
    logWithTimestamp(`Found ${holidayDates.length} holidays in calendar`);
    
    // Look back up to 10 days to find the required business days
    let maxLookback = 10;
    let lookbackCounter = 0;
    
    while (daysFound < daysBack && lookbackCounter < maxLookback) {
      const dateStr = currentDate.format('YYYY-MM-DD');
      const dayOfWeek = currentDate.day(); // 0 = Sunday, 6 = Saturday
      
      // Check if it's a weekend
      const isWeekend = dayOfWeek === 0 || dayOfWeek === 6;
      
      // Check if it's a holiday
      const isHoliday = holidayDates.includes(dateStr);
      
      // If it's a business day (not weekend, not holiday)
      if (!isWeekend && !isHoliday) {
        businessDays.push({
          date: dateStr,
          dayOfWeek: currentDate.format('dddd'),
          dayNumber: currentDate.day(),
          isBusinessDay: true
        });
        daysFound++;
        logWithTimestamp(`Business day ${daysFound}: ${dateStr} (${currentDate.format('dddd')})`);
      } else {
        logWithTimestamp(`Skipping ${dateStr} (${currentDate.format('dddd')}) - ${isWeekend ? 'Weekend' : 'Holiday'}`);
      }
      
      currentDate.subtract(1, 'day');
      lookbackCounter++;
    }
    
    if (daysFound < daysBack) {
      logWithTimestamp(`Warning: Only found ${daysFound} business days in last ${maxLookback} days`, 'WARN');
    }
    
    // Return in chronological order (oldest first)
    return businessDays.reverse();
    
  } catch (error) {
    logWithTimestamp(`Error calculating business days: ${error.message}`, 'ERROR');
    throw error;
  }
}

/**
 * GET /smg-daily-dates
 * Returns the last 3 business days for SMG data processing
 */
app.get('/smg-daily-dates', async (req, res) => {
  try {
    logWithTimestamp('=== SMG DAILY DATES REQUEST ===');
    
    // Get days parameter (default 3)
    const daysBack = parseInt(req.query.days) || 3;
    
    if (daysBack < 1 || daysBack > 10) {
      return res.status(400).json({
        success: false,
        error: 'Days parameter must be between 1 and 10',
        timestamp: moment().toISOString()
      });
    }
    
    logWithTimestamp(`Requested ${daysBack} business days`);
    
    const businessDays = await calculateBusinessDays(daysBack);
    
    const response = {
      success: true,
      requestedDays: daysBack,
      foundDays: businessDays.length,
      businessDays: businessDays,
      dateRange: {
        startDate: businessDays[0]?.date,
        endDate: businessDays[businessDays.length - 1]?.date
      },
      timestamp: moment().toISOString(),
      nextSteps: [
        'Use these dates with /smg-download endpoint',
        'Transform CSV data with /smg-transform',
        'Upload to Supabase with /smg-upload'
      ]
    };
    
    logWithTimestamp(`Successfully calculated ${businessDays.length} business days`);
    res.json(response);
    
  } catch (error) {
    logWithTimestamp(`Error in /smg-daily-dates: ${error.message}`, 'ERROR');
    res.status(500).json({
      success: false,
      error: error.message,
      timestamp: moment().toISOString()
    });
  }
});

/**
 * POST /smg-daily-dates
 * Calculate business days for a specific date range
 */
app.post('/smg-daily-dates', async (req, res) => {
  try {
    logWithTimestamp('=== SMG DAILY DATES POST REQUEST ===');
    
    const { startDate, endDate, excludeWeekends = true, excludeHolidays = true } = req.body;
    
    if (!startDate || !endDate) {
      return res.status(400).json({
        success: false,
        error: 'startDate and endDate are required',
        timestamp: moment().toISOString()
      });
    }
    
    const start = moment(startDate);
    const end = moment(endDate);
    
    if (!start.isValid() || !end.isValid()) {
      return res.status(400).json({
        success: false,
        error: 'Invalid date format. Use YYYY-MM-DD',
        timestamp: moment().toISOString()
      });
    }
    
    if (start.isAfter(end)) {
      return res.status(400).json({
        success: false,
        error: 'startDate must be before or equal to endDate',
        timestamp: moment().toISOString()
      });
    }
    
    logWithTimestamp(`Custom date range: ${startDate} to ${endDate}`);
    
    // Get holidays if excluding them
    let holidayDates = [];
    if (excludeHolidays) {
      const { data: holidays, error: holidayError } = await supabase
        .from('calendar')
        .select('date')
        .eq('is_holiday', true)
        .gte('date', startDate)
        .lte('date', endDate);
      
      if (!holidayError && holidays) {
        holidayDates = holidays.map(h => h.date);
      }
    }
    
    const datesList = [];
    let currentDate = moment(start);
    
    while (currentDate.isSameOrBefore(end)) {
      const dateStr = currentDate.format('YYYY-MM-DD');
      const dayOfWeek = currentDate.day();
      const isWeekend = dayOfWeek === 0 || dayOfWeek === 6;
      const isHoliday = holidayDates.includes(dateStr);
      
      let includeDate = true;
      let skipReason = null;
      
      if (excludeWeekends && isWeekend) {
        includeDate = false;
        skipReason = 'Weekend';
      } else if (excludeHolidays && isHoliday) {
        includeDate = false;
        skipReason = 'Holiday';
      }
      
      datesList.push({
        date: dateStr,
        dayOfWeek: currentDate.format('dddd'),
        isBusinessDay: includeDate,
        isWeekend: isWeekend,
        isHoliday: isHoliday,
        skipReason: skipReason
      });
      
      currentDate.add(1, 'day');
    }
    
    const businessDays = datesList.filter(d => d.isBusinessDay);
    
    const response = {
      success: true,
      dateRange: { startDate, endDate },
      filters: { excludeWeekends, excludeHolidays },
      totalDays: datesList.length,
      businessDays: businessDays.length,
      skippedDays: datesList.length - businessDays.length,
      dates: datesList,
      businessDatesOnly: businessDays.map(d => d.date),
      timestamp: moment().toISOString()
    };
    
    logWithTimestamp(`Custom range: ${businessDays.length} business days found`);
    res.json(response);
    
  } catch (error) {
    logWithTimestamp(`Error in POST /smg-daily-dates: ${error.message}`, 'ERROR');
    res.status(500).json({
      success: false,
      error: error.message,
      timestamp: moment().toISOString()
    });
  }
});

// ========================================
// HEALTH CHECK AND STATUS ENDPOINTS
// ========================================

app.get('/', (req, res) => {
  res.json({
    service: 'SMG Cloud Automation Pipeline',
    phase: 'Phase 2 - Modular Components',
    status: 'Building',
    modules: {
      '/smg-daily-dates': 'COMPLETED ✅',
      '/smg-transform': 'Building... 🚧',
      '/smg-upload': 'Pending... ⏳',
      '/smg-pipeline': 'Pending... ⏳',
      '/smg-status': 'Pending... ⏳'
    },
    timestamp: moment().toISOString()
  });
});

app.get('/health', (req, res) => {
  res.json({
    status: 'healthy',
    timestamp: moment().toISOString(),
    uptime: process.uptime(),
    memory: process.memoryUsage(),
    environment: process.env.NODE_ENV || 'development'
  });
});

// Error handling middleware
app.use((err, req, res, next) => {
  logWithTimestamp(`Unhandled error: ${err.message}`, 'ERROR');
  res.status(500).json({
    success: false,
    error: 'Internal server error',
    timestamp: moment().toISOString()
  });
});

// 404 handler
app.use((req, res) => {
  logWithTimestamp(`404 - Route not found: ${req.method} ${req.path}`, 'WARN');
  res.status(404).json({
    success: false,
    error: 'Endpoint not found',
    availableEndpoints: [
      'GET /',
      'GET /health',
      'GET /smg-daily-dates',
      'POST /smg-daily-dates'
    ],
    timestamp: moment().toISOString()
  });
});

// Start server
app.listen(PORT, () => {
  logWithTimestamp(`SMG Cloud Automation Pipeline listening on port ${PORT}`);
  logWithTimestamp('Phase 2 - Module 1 (SMG Daily Dates) ready for testing');
});

module.exports = app;