#!/usr/bin/env python3

import argparse
from datetime import datetime
import re
from dateutil import parser, tz
from calendar_analyzer import CalendarAnalyzer

def valid_date(s):
    """Convert string to datetime, used for argument parsing."""
    try:
        return parser.parse(s).replace(tzinfo=tz.gettz('America/Los_Angeles'))
    except ValueError:
        msg = f"Not a valid date: '{s}'"
        raise argparse.ArgumentTypeError(msg)

def main():
    parser = argparse.ArgumentParser(
        description='Analyze calendar events within a timeframe with optional regex filtering.'
    )

    # Required calendar file argument
    parser.add_argument(
        'calendar_file',
        help='Name of the .ics file in the /calendars directory'
    )

    # Date range arguments
    parser.add_argument(
        '--start',
        type=valid_date,
        help='Start date (e.g., "2024-01-01" or "30 days ago")'
    )
    parser.add_argument(
        '--end',
        type=valid_date,
        help='End date (e.g., "2024-12-31" or "today")'
    )

    # Event filtering
    parser.add_argument(
        '--pattern',
        help='Regex pattern to filter events (searches both summary and description)'
    )

    # Analysis options - these are kept for backward compatibility but no longer used
    parser.add_argument(
        '--show-day-stats',
        action='store_true',
        help='Show event distribution by day of week (always shown)'
    )
    parser.add_argument(
        '--show-monthly-stats',
        action='store_true',
        help='Show monthly statistics including total hours, average hours per week, and event counts (always shown)'
    )
    parser.add_argument(
        '--show-weekly-stats',
        action='store_true',
        help='Show weekly statistics including total and average hours per week (always shown)'
    )

    args = parser.parse_args()

    # Set default dates if not provided - no time limit by default
    now = datetime.now(tz.gettz('America/Los_Angeles'))
    if args.end is None:
        args.end = now
    if args.start is None:
        args.start = datetime(1970, 1, 1, tzinfo=tz.gettz('America/Los_Angeles'))

    # Initialize analyzer with the specified calendar file
    analyzer = CalendarAnalyzer(args.calendar_file)

    # Create patterns dict based on input
    if args.pattern:
        pattern = re.compile(args.pattern, re.IGNORECASE)
        patterns = {'matched_events': pattern}
    else:
        # Default patterns if no specific pattern is provided
        patterns = {
            'meetings': re.compile(r'meeting|sync|standup', re.IGNORECASE),
            'classes': re.compile(r'CSE \d+', re.IGNORECASE),
            'workout': re.compile(r'workout', re.IGNORECASE),
            'social': re.compile(r'lunch|coffee', re.IGNORECASE),
        }

    try:
        # Analyze events
        results = analyzer.analyze_events(args.start, args.end, patterns)

        # Print basic results
        print(f"\nAnalyzing events from {args.start.strftime('%Y-%m-%d')} to {args.end.strftime('%Y-%m-%d')}")

        # Show individual events if requested
        for pattern_name, events in results.items():
            print(f"\n{pattern_name.title()} ({len(events)} events):")
            for dt, summary, duration in events:
                print(f"{dt.strftime('%Y-%m-%d %H:%M')} - {summary} ({duration.total_seconds()/3600:.1f}h)")

        # Always show time spent
        time_spent = analyzer.get_time_spent(results)
        print("\nTime Spent on Events:")
        for pattern_name, duration in time_spent.items():
            hours = duration.total_seconds() / 3600
            print(f"{pattern_name.title()}: {hours:.1f} hours")

        # Always show all analyses

        # Day statistics
        distribution = analyzer.get_day_stats(results)
        print("\nEvent Distribution by Day:")
        for pattern_name, dist in distribution.items():
            print(f"\n{pattern_name.title()}:")
            for day, stats in dist.items():
                count = stats['count']
                total = stats['total_hours']
                avg = stats['avg_hours']
                print(f"  {day:9} - {count:2d} events, {total:5.1f} hours total ({avg:4.1f}h avg/event)")

        # Monthly statistics
        monthly_stats = analyzer.get_monthly_stats(results)
        print("\nMonthly Statistics:")
        for pattern_name, stats in monthly_stats.items():
            print(f"\n{pattern_name.title()}:")
            for month, month_stats in sorted(stats.items()):
                print(f"  Month of {month}: {month_stats['total_hours']:.1f} total hours "
                      f"({month_stats['avg_hours']:.1f}h avg/week), {month_stats['event_count']} events")

        # Weekly statistics
        weekly_stats = analyzer.get_weekly_stats(results)
        print("\nWeekly Statistics:")
        for pattern_name, stats in weekly_stats.items():
            print(f"\n{pattern_name.title()}:")
            for week, week_stats in sorted(stats.items()):
                print(f"  Week of {week}: {week_stats['total_hours']:.1f} total hours ({week_stats['avg_hours']:.1f}h avg/day)")

    except FileNotFoundError:
        print(f"Error: Calendar file '{args.calendar_file}' not found in /calendars directory")
        return 1
    except Exception as e:
        print(f"Error analyzing calendar: {str(e)}")
        return 1

    return 0

if __name__ == '__main__':
    exit(main())
