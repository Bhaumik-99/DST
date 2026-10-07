package cron

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Schedule calculates the next execution time for a job.
type Schedule interface {
	Next(from time.Time) time.Time
}

// IntervalSchedule handles macros like "@every 5s".
type IntervalSchedule struct {
	Interval time.Duration
}

func (s IntervalSchedule) Next(from time.Time) time.Time {
	return from.Add(s.Interval)
}

// CronSchedule handles 5 or 6 field cron expressions.
// 6-field: sec min hour dom month dow
// 5-field: min hour dom month dow (sec defaults to 0)
type CronSchedule struct {
	Seconds []int // 0-59
	Minutes []int // 0-59
	Hours   []int // 0-23
	Days    []int // 1-31
	Months  []int // 1-12
	DaysOfWeek []int // 0-6 (0 = Sunday)
}

func (c *CronSchedule) Next(from time.Time) time.Time {
	t := from.Add(time.Second).Truncate(time.Second)

	// Search up to 5 years in future
	limit := from.AddDate(5, 0, 0)
	for t.Before(limit) {
		if !contains(c.Months, int(t.Month())) {
			t = time.Date(t.Year(), t.Month()+1, 1, 0, 0, 0, 0, t.Location())
			continue
		}
		if !contains(c.Days, t.Day()) || !contains(c.DaysOfWeek, int(t.Weekday())) {
			t = time.Date(t.Year(), t.Month(), t.Day()+1, 0, 0, 0, 0, t.Location())
			continue
		}
		if !contains(c.Hours, t.Hour()) {
			t = time.Date(t.Year(), t.Month(), t.Day(), t.Hour()+1, 0, 0, 0, t.Location())
			continue
		}
		if !contains(c.Minutes, t.Minute()) {
			t = time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute()+1, 0, 0, t.Location())
			continue
		}
		if !contains(c.Seconds, t.Second()) {
			t = t.Add(time.Second)
			continue
		}
		return t
	}
	return time.Time{}
}

func contains(slice []int, val int) bool {
	for _, item := range slice {
		if item == val {
			return true
		}
	}
	return false
}

// Parse parses standard cron expressions and @every syntax.
func Parse(expr string) (Schedule, error) {
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return nil, fmt.Errorf("empty cron expression")
	}

	// Check for @every macro
	if strings.HasPrefix(expr, "@every ") {
		durStr := strings.TrimSpace(strings.TrimPrefix(expr, "@every "))
		dur, err := time.ParseDuration(durStr)
		if err != nil {
			return nil, fmt.Errorf("invalid @every duration %q: %w", durStr, err)
		}
		if dur <= 0 {
			return nil, fmt.Errorf("duration must be positive")
		}
		return IntervalSchedule{Interval: dur}, nil
	}

	// Common shortcuts
	switch expr {
	case "@hourly":
		expr = "0 * * * *"
	case "@daily", "@midnight":
		expr = "0 0 * * *"
	case "@weekly":
		expr = "0 0 * * 0"
	case "@monthly":
		expr = "0 0 1 * *"
	case "@yearly", "@annually":
		expr = "0 0 1 1 *"
	}

	parts := strings.Fields(expr)
	var secField, minField, hourField, domField, monthField, dowField string

	if len(parts) == 5 {
		secField = "0"
		minField, hourField, domField, monthField, dowField = parts[0], parts[1], parts[2], parts[3], parts[4]
	} else if len(parts) == 6 {
		secField, minField, hourField, domField, monthField, dowField = parts[0], parts[1], parts[2], parts[3], parts[4], parts[5]
	} else {
		return nil, fmt.Errorf("cron expression requires 5 or 6 fields, got %d", len(parts))
	}

	secs, err := parseField(secField, 0, 59)
	if err != nil {
		return nil, fmt.Errorf("seconds: %w", err)
	}
	mins, err := parseField(minField, 0, 59)
	if err != nil {
		return nil, fmt.Errorf("minutes: %w", err)
	}
	hours, err := parseField(hourField, 0, 23)
	if err != nil {
		return nil, fmt.Errorf("hours: %w", err)
	}
	days, err := parseField(domField, 1, 31)
	if err != nil {
		return nil, fmt.Errorf("days: %w", err)
	}
	months, err := parseField(monthField, 1, 12)
	if err != nil {
		return nil, fmt.Errorf("months: %w", err)
	}
	dows, err := parseField(dowField, 0, 7)
	if err != nil {
		return nil, fmt.Errorf("day of week: %w", err)
	}
	// Normalize Sunday (7 -> 0)
	for i, d := range dows {
		if d == 7 {
			dows[i] = 0
		}
	}

	return &CronSchedule{
		Seconds:    secs,
		Minutes:    mins,
		Hours:      hours,
		Days:       days,
		Months:     months,
		DaysOfWeek: dows,
	}, nil
}

func parseField(field string, min, max int) ([]int, error) {
	if field == "*" || field == "?" {
		res := make([]int, max-min+1)
		for i := range res {
			res[i] = min + i
		}
		return res, nil
	}

	seen := make(map[int]bool)
	parts := strings.Split(field, ",")
	for _, part := range parts {
		if strings.Contains(part, "/") {
			// step expression, e.g., */5 or 10-30/2
			slashParts := strings.Split(part, "/")
			if len(slashParts) != 2 {
				return nil, fmt.Errorf("invalid step syntax in %q", part)
			}
			step, err := strconv.Atoi(slashParts[1])
			if err != nil || step <= 0 {
				return nil, fmt.Errorf("invalid step value in %q", part)
			}

			start := min
			end := max
			if slashParts[0] != "*" {
				if strings.Contains(slashParts[0], "-") {
					rangeParts := strings.Split(slashParts[0], "-")
					start, _ = strconv.Atoi(rangeParts[0])
					end, _ = strconv.Atoi(rangeParts[1])
				} else {
					start, _ = strconv.Atoi(slashParts[0])
				}
			}

			for i := start; i <= end; i += step {
				if i >= min && i <= max {
					seen[i] = true
				}
			}
		} else if strings.Contains(part, "-") {
			rangeParts := strings.Split(part, "-")
			start, err1 := strconv.Atoi(rangeParts[0])
			end, err2 := strconv.Atoi(rangeParts[1])
			if err1 != nil || err2 != nil || start > end || start < min || end > max {
				return nil, fmt.Errorf("invalid range %q", part)
			}
			for i := start; i <= end; i++ {
				seen[i] = true
			}
		} else {
			val, err := strconv.Atoi(part)
			if err != nil || val < min || val > max {
				return nil, fmt.Errorf("invalid value %q (must be %d-%d)", part, min, max)
			}
			seen[val] = true
		}
	}

	var result []int
	for i := min; i <= max; i++ {
		if seen[i] {
			result = append(result, i)
		}
	}
	return result, nil
}
