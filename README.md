# iCalendar Event Analyzer

A Python tool for analyzing iCalendar (.ics) files and aggregating events based on regex patterns within a specified timeframe. Features powerful command-line interface with data visualization capabilities.

## Features

- Read and parse iCalendar (.ics) files from a specified directory
- Match events using customizable regex patterns
- Filter events within a specific time range
- Support for timezone-aware datetime handling
- Aggregated results by pattern categories
- Visual analysis with ASCII charts showing:
  - Event distribution by day of week
  - Total time spent by category
  - Weekly hours over time
  - Monthly statistics and trends
- Command-line interface for easy analysis
- Comprehensive test suite with pytest

## How to run calendar_analyzer.py
python calendar_analyzer.py example.ics example2.ics --start YYYY-MM-DD --end YYYY-MM-DD

## Installation

1. Clone this repository
2. Install dependencies:
```bash
pip install -r requirements.txt
```

## Dependencies

- icalendar >= 5.0.0: iCalendar file parsing
- python-dateutil >= 2.8.2: Advanced date handling
- pytz >= 2023.3: Timezone support
- pytest >= 7.4.0: Testing framework
- termplotlib >= 0.3.5: Terminal-based plotting

## Usage

### As a Python Library

```python
from calendar_analyzer import CalendarAnalyzer
from datetime import datetime
from dateutil import tz
import re

# Initialize analyzer
analyzer = CalendarAnalyzer(calendar_dir="/path/to/calendar/files")

# Define timeframe (optional - defaults to all events)
start_time = datetime(2024, 12, 1, tzinfo=tz.gettz('America/Los_Angeles'))
end_time = datetime(2024, 12, 31, tzinfo=tz.gettz('America/Los_Angeles'))

# Define patterns
patterns = {
    'meetings': re.compile(r'meeting|sync|standup', re.IGNORECASE),
    'social': re.compile(r'lunch|dinner|party', re.IGNORECASE),
}

# Analyze events
results = analyzer.analyze_events(start_time, end_time, patterns)

# Get various statistics
time_spent = analyzer.get_time_spent(results)  # Always included
day_stats = analyzer.get_day_stats(results)
monthly_stats = analyzer.get_monthly_stats(results)
weekly_stats = analyzer.get_weekly_stats(results)

# Process results
for pattern_name, events in results.items():
    print(f"\n{pattern_name.title()} ({len(events)} events):")
    for dt, summary, duration in events:
        print(f"{dt.strftime('%Y-%m-%d %H:%M')} - {summary} ({duration.total_seconds()/3600:.1f}h)")
```

### Command Line Interface

The tool provides a powerful CLI for analyzing calendar events:

```bash
python cli.py calendars/calendar.ics [--start "2024-01-01"] [--end "2024-12-31"] \
    [--pattern "meeting|sync|standup"] \
    [--show-day-stats] [--show-monthly-stats] \
    [--show-weekly-stats] [--show-graphs]
```

#### CLI Options:
- `calendar_file`: Name of the .ics file to analyze
- `--start`: Analysis start date (e.g., "2024-01-01" or "30 days ago"). Optional - defaults to all events
- `--end`: Analysis end date (e.g., "2024-12-31" or "today"). Optional - defaults to current time
- `--pattern`: Regex pattern to filter events (searches both summary and description)
- `--show-day-stats`: Show event distribution by day of week
- `--show-monthly-stats`: Show monthly statistics including total hours, average hours per day, and event counts
- `--show-weekly-stats`: Show weekly statistics including total and average hours per week
- `--show-graphs`: Show visualizations of the analyses

Note: Time spent analysis is always included in the output.

The tool provides visual analysis through ASCII charts showing:
- Event counts by day of week
- Hours spent by day of week
- Total hours by category
- Weekly hours over time
- Monthly trends and patterns

## Testing

The project includes a comprehensive test suite using pytest. Tests cover:

- Calendar file loading and parsing
- Event analysis and pattern matching
- Day of week distribution statistics
- Time spent calculations
- Weekly statistics analysis
- Monthly statistics analysis
- All-day event handling

Run tests using:
```bash
pytest
```

## Contributing

1. Fork the repository
2. Create your feature branch
3. Write tests for new features
4. Ensure all tests pass
5. Submit a pull request

## License

This project is licensed under the MIT License - see the LICENSE file for details.
