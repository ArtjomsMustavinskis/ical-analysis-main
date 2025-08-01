import pytest
from datetime import datetime, timedelta
import re
from calendar_analyzer import CalendarAnalyzer
from dateutil import tz
import random
from icalendar import Calendar, Event
import os
import tempfile

def create_sample_calendar(num_events=15):
    """Create a sample calendar with random events."""
    cal = Calendar()
    cal.add('prodid', '-//Sample Calendar//')
    cal.add('version', '2.0')

    event_types = [
        "CSE 6040 - Lecture",
        "CSE 6040 - Office Hours",
        "Email / Slack catchup",
        "Lunch with team",
        "Workout: Run",
        "Workout: Gym",
        "Team Meeting",
        "1:1 Meeting",
        "Project Sync",
        "Coffee Break"
    ]

    # Create a range of dates between Jan 2024 and Dec 2024
    start_date = datetime(2024, 1, 1, tzinfo=tz.gettz('America/Los_Angeles'))
    end_date = datetime(2024, 12, 31, tzinfo=tz.gettz('America/Los_Angeles'))

    events = []
    for _ in range(num_events):
        event = Event()

        # Random date and time
        random_days = random.randint(0, (end_date - start_date).days)
        random_hours = random.randint(9, 17)  # Business hours
        event_date = start_date + timedelta(days=random_days)
        event_datetime = event_date.replace(hour=random_hours, minute=random.randint(0, 59))

        event.add('summary', random.choice(event_types))
        event.add('dtstart', event_datetime)
        event.add('dtend', event_datetime + timedelta(hours=1))
        event.add('dtstamp', event_datetime)
        event.add('uid', f'{random.randint(1000000, 9999999)}@example.com')

        # Add description to some events
        if random.random() < 0.5:
            event.add('description', f"Details for {event['summary']}")

        events.append(event)
        cal.add_component(event)

    return cal

@pytest.fixture
def sample_calendar_file(tmp_path):
    """Create a temporary calendar file."""
    cal = create_sample_calendar()
    cal_path = tmp_path / 'sample.ics'
    with open(cal_path, 'wb') as f:
        f.write(cal.to_ical())
    return cal_path

@pytest.fixture
def analyzer(sample_calendar_file):
    return CalendarAnalyzer(str(sample_calendar_file))

@pytest.fixture
def sample_patterns():
    return {
        'classes': re.compile(r'CSE \d+', re.IGNORECASE),
        'meetings': re.compile(r'meeting|sync', re.IGNORECASE),
        'workout': re.compile(r'workout', re.IGNORECASE),
        'social': re.compile(r'lunch|coffee', re.IGNORECASE),
        'communication': re.compile(r'email|slack', re.IGNORECASE)
    }

@pytest.fixture
def timeframe():
    tz_info = tz.gettz('America/Los_Angeles')
    return (
        datetime(2024, 1, 1, 0, 0, 0, tzinfo=tz_info),
        datetime(2024, 12, 31, 23, 59, 59, tzinfo=tz_info)
    )

def test_analyzer_initialization(analyzer, sample_calendar_file):
    assert os.path.exists(analyzer.calendar_file)
    assert analyzer.local_tz is not None

def test_calendar_loading(analyzer):
    cal = analyzer.calendar
    assert isinstance(cal, Calendar)
    assert len(list(cal.walk('VEVENT'))) > 0

def test_event_analysis(analyzer, sample_patterns, timeframe):
    start_time, end_time = timeframe
    results = analyzer.analyze_events(start_time, end_time, sample_patterns)

    # Verify we have results for each pattern
    assert all(pattern_name in results for pattern_name in sample_patterns)

    # Verify all events are within the timeframe
    for events in results.values():
        for dt, summary, duration in events:
            assert start_time <= dt <= end_time
            assert isinstance(duration, timedelta)

    # Print summary of found events
    print("\nFound events:")
    for pattern_name, events in results.items():
        print(f"\n{pattern_name.title()} ({len(events)} events):")
        for dt, summary, duration in events:
            print(f"{dt.strftime('%Y-%m-%d %H:%M')} - {summary} ({duration.total_seconds()/3600:.1f}h)")

def test_day_stats(analyzer, sample_patterns, timeframe):
    start_time, end_time = timeframe
    results = analyzer.analyze_events(start_time, end_time, sample_patterns)
    distribution = analyzer.get_day_stats(results)

    # Verify we have distribution for each pattern
    assert all(pattern_name in distribution for pattern_name in sample_patterns)

    # Verify we have all days of the week with required stats
    days = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    required_stats = ['count', 'total_hours', 'avg_hours']
    assert all(
        all(
            all(stat in dist[day] for stat in required_stats)
            for day in days
        )
        for dist in distribution.values()
    )

    print("\nEvent distribution by day:")
    for pattern, dist in distribution.items():
        print(f"\n{pattern.title()}:")
        for day, stats in dist.items():
            count = stats['count']
            total = stats['total_hours']
            avg = stats['avg_hours']
            print(f"  {day:9} - {count:2d} events, {total:5.1f} hours total ({avg:4.1f}h avg/event)")

def test_time_spent(analyzer, sample_patterns, timeframe):
    start_time, end_time = timeframe
    results = analyzer.analyze_events(start_time, end_time, sample_patterns)
    time_spent = analyzer.get_time_spent(results)

    # Verify we have time spent for each pattern
    assert all(pattern_name in time_spent for pattern_name in sample_patterns)

    print("\nTime spent on each event type:")
    for pattern, duration in time_spent.items():
        hours = duration.total_seconds() / 3600
        print(f"{pattern.title()}: {hours:.1f} hours")

def test_pattern_matching(analyzer, timeframe):
    start_time, end_time = timeframe
    pattern = re.compile(r'CSE 6040', re.IGNORECASE)
    results = analyzer.analyze_events(start_time, end_time, {'cse_events': pattern})

    # Verify all matched events contain the pattern
    for events in results.values():
        for _, summary, _ in events:
            assert pattern.search(summary) is not None

    print("\nCSE 6040 events:")
    for events in results.values():
        for dt, summary, duration in events:
            print(f"{dt.strftime('%Y-%m-%d %H:%M')} - {summary} ({duration.total_seconds()/3600:.1f}h)")

def test_all_day_events(analyzer, timeframe):
    """Test that all-day events are handled correctly with zero duration."""
    # Create a test calendar with an all-day event
    cal = Calendar()
    cal.add('prodid', '-//Test Calendar//')
    cal.add('version', '2.0')

    # Add an all-day event
    event = Event()
    event.add('summary', 'All Day Meeting')
    event.add('dtstart', datetime(2024, 1, 15).date())  # Note: using .date() makes it all-day
    event.add('dtend', datetime(2024, 1, 16).date())
    cal.add_component(event)

    # Write to a temporary file
    with tempfile.NamedTemporaryFile(mode='wb', suffix='.ics', delete=False) as f:
        f.write(cal.to_ical())
        temp_file = f.name

    # Create analyzer with our test calendar
    test_analyzer = CalendarAnalyzer(temp_file)

    # Define a pattern that will match our event
    patterns = {'meetings': re.compile(r'meeting', re.IGNORECASE)}

    # Analyze events
    start_time, end_time = timeframe
    results = test_analyzer.analyze_events(start_time, end_time, patterns)

    # Get time spent
    time_spent = test_analyzer.get_time_spent(results)

    # Verify the all-day event has zero duration
    assert time_spent['meetings'] == timedelta(0), "All-day event should have zero duration"

    # Clean up
    os.unlink(temp_file)

def test_weekly_stats(analyzer, sample_patterns, timeframe):
    start_time, end_time = timeframe
    results = analyzer.analyze_events(start_time, end_time, sample_patterns)
    weekly_stats = analyzer.get_weekly_stats(results)

    # Verify we have weekly stats for each pattern
    assert all(pattern_name in weekly_stats for pattern_name in sample_patterns)

    # Verify each week has required stats
    required_stats = ['total_hours', 'avg_hours']
    assert all(
        all(stat in week_stats for stat in required_stats)
        for pattern_stats in weekly_stats.values()
        for week_stats in pattern_stats.values()
    )

    print("\nWeekly statistics:")
    for pattern, stats in weekly_stats.items():
        print(f"\n{pattern.title()}:")
        for week, week_stats in sorted(stats.items()):
            print(f"  Week of {week}: {week_stats['total_hours']:.1f} total hours ({week_stats['avg_hours']:.1f}h avg/day)")

def test_monthly_stats(analyzer, sample_patterns, timeframe):
    start_time, end_time = timeframe
    results = analyzer.analyze_events(start_time, end_time, sample_patterns)
    monthly_stats = analyzer.get_monthly_stats(results)

    # Verify we have monthly stats for each pattern
    assert all(pattern_name in monthly_stats for pattern_name in sample_patterns)

    # Verify each month has required stats
    required_stats = ['total_hours', 'avg_hours', 'event_count']
    assert all(
        all(stat in month_stats for stat in required_stats)
        for pattern_stats in monthly_stats.values()
        for month_stats in pattern_stats.values()
    )

    # Verify month keys are in correct format (YYYY-MM)
    assert all(
        all(re.match(r'^\d{4}-\d{2}$', month_key) for month_key in pattern_stats.keys())
        for pattern_stats in monthly_stats.values()
    )

    print("\nMonthly statistics:")
    for pattern, stats in monthly_stats.items():
        print(f"\n{pattern.title()}:")
        for month, month_stats in sorted(stats.items()):
            print(f"  Month of {month}: {month_stats['total_hours']:.1f} total hours "
                  f"({month_stats['avg_hours']:.1f}h avg/day), {month_stats['event_count']} events")
