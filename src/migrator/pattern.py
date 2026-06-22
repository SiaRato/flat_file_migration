"""Filename generation pattern parser (Functional).

Parses templates like 'report_{date:%Y%m%d}_{seq:1-5000:04d}.csv'
to generate a stream of predictable filenames over a time window.
"""

import re
from datetime import datetime, timedelta, timezone
from typing import Generator


def generate_filenames(template: str, lookback_days: int) -> Generator[str, None, None]:
    """Generate filenames based on a date and sequence template.
    
    Args:
        template: The filename pattern, e.g. 'data_{date:%Y-%m-%d}_{seq:1-5000:04d}.json'
        lookback_days: Number of days to look back into the past (0 = today only).
        
    Yields:
        Predictable filename strings.
        
    Raises:
        ValueError: If the template is malformed.
    """
    # Parse the sequence block if it exists
    # Matches {seq:start-end:padding} or {seq:start-end}
    seq_match = re.search(r'\{seq:(\d+)-(\d+):?([^}]*)\}', template)
    
    start_seq = 1
    end_seq = 1
    has_seq = False
    
    if seq_match:
        has_seq = True
        start_seq = int(seq_match.group(1))
        end_seq = int(seq_match.group(2))
        pad_fmt = seq_match.group(3)
        
        # Replace the custom {seq:...} block with a standard Python format variable {seq_val}
        fmt_str = f"{{seq_val:{pad_fmt}}}" if pad_fmt else "{seq_val}"
        template = template[:seq_match.start()] + fmt_str + template[seq_match.end():]

    now = datetime.now(timezone.utc)
    
    # Iterate from oldest to newest, so the migration processes chronologically
    for day_offset in reversed(range(lookback_days + 1)):
        current_date = now - timedelta(days=day_offset)
        
        if has_seq:
            for i in range(start_seq, end_seq + 1):
                try:
                    yield template.format(date=current_date, seq_val=i)
                except KeyError as e:
                    raise ValueError(f"Invalid template variable: {e} in '{template}'. Only {{date}} and {{seq}} are supported.")
                except ValueError as e:
                    raise ValueError(f"Invalid format specifier in '{template}': {e}")
        else:
            try:
                yield template.format(date=current_date)
            except KeyError as e:
                raise ValueError(f"Invalid template variable: {e} in '{template}'. Only {{date}} is supported if there is no {{seq}} block.")
            except ValueError as e:
                raise ValueError(f"Invalid format specifier in '{template}': {e}")
