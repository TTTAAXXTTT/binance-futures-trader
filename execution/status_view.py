"""Pure display derivation; never grants execution permission or performs I/O."""
from __future__ import annotations

import copy
import math


def signal_windows(markets, now_ms, upper_ms=None):
    result = copy.deepcopy(markets or {})
    upper_ms = now_ms if upper_ms is None else upper_ms
    for market in result.values():
        if not isinstance(market, dict):
            continue
        boundary = market.get('boundary_ms')
        valid_time = (type(boundary) is int and type(now_ms) is int and
                      type(upper_ms) is int and boundary <= now_ms and
                      upper_ms < boundary + 300000)
        market['signal_valid_now'] = bool(market.get('direction') and valid_time)
        market['signal_window_end_ms'] = boundary + 300000 if type(boundary) is int else None
    return result


def age_status(snapshot, elapsed_seconds):
    """Advance a published copy by monotonic elapsed time, preserving uncertainty."""
    result = copy.deepcopy(snapshot)
    elapsed = max(0.0, float(elapsed_seconds))
    milliseconds = round(elapsed * 1000)
    metadata = result.get('snapshot_metadata')
    if isinstance(metadata, dict) and type(metadata.get('age_seconds')) in (float, int):
        metadata['age_seconds'] += elapsed
    for key in ('server_time_ms', 'server_time_upper_ms'):
        if type(result.get(key)) is int:
            result[key] += milliseconds
    result['markets'] = signal_windows(result.get('markets'), result.get('server_time_ms'),
                                      result.get('server_time_upper_ms'))
    account = result.get('account_snapshot')
    if isinstance(account, dict) and type(account.get('age_seconds')) in (float, int):
        account['age_seconds'] += elapsed
    clock = result.get('exchange_clock')
    if isinstance(clock, dict) and type(clock.get('sync_age_ms')) in (float, int):
        clock['sync_age_ms'] += milliseconds
    for key in ('network_retry', 'execution_recovery', 'funding_read', 'exchange_clock'):
        state = result.get(key)
        if isinstance(state, dict) and type(state.get('retry_in_seconds')) in (float, int):
            state['retry_in_seconds'] = max(0, state['retry_in_seconds'] - elapsed)
    stream = result.get('market_stream')
    if isinstance(stream, dict) and elapsed:
        age_market_stream(stream, elapsed)
    check = result.get('connection_check')
    if isinstance(check, dict):
        result['connection_check'] = age_connection_check(check, result.get('server_time_ms'))
    return result


def finite_number(value):
    return type(value) in (int, float) and math.isfinite(value)


def age_market_stream(stream, elapsed):
    """Age only a display copy; expiry says nothing about socket connectivity."""
    copy_age = stream.get('display_copy_age_seconds', 0)
    stream['display_copy_age_seconds'] = (copy_age if finite_number(copy_age) else 0) + elapsed
    copy_expired = stream['display_copy_age_seconds'] > 3
    stream['display_copy_stale'] = copy_expired
    stream['stale'] = bool(stream.get('stale')) or copy_expired
    for retry in (stream.get('retry') or {}).values():
        if isinstance(retry, dict) and finite_number(retry.get('retry_in_seconds')):
            retry['retry_in_seconds'] = max(0, retry['retry_in_seconds'] - elapsed)
    for symbol in (stream.get('symbols') or {}).values():
        if not isinstance(symbol, dict):
            continue
        freshness = symbol.get('freshness')
        for kind in ('book', 'trade', 'mark'):
            flag = kind + '_live'
            detail = freshness.get(kind) if isinstance(freshness, dict) else None
            if isinstance(detail, dict):
                for key in ('receive_age_ms', 'event_age_upper_ms'):
                    if finite_number(detail.get(key)):
                        detail[key] += round(elapsed * 1000)
                expired = copy_expired or any(finite_number(detail.get(key)) and detail[key] > 3000
                    for key in ('receive_age_ms', 'event_age_upper_ms'))
                detail['display_copy_expired'] = expired
                detail['source_available'] = detail.get('source_available', detail.get('available') is True)
                if expired:
                    detail['available'] = False
                    detail['display_reason'] = 'display_copy_expired'
                symbol[flag] = bool(symbol.get(flag)) and detail.get('available') is True
            elif copy_expired:
                # Older snapshots have no independent event/receive ages.
                symbol[flag] = False
    # latest_trade is historical evidence after expiry and must not be labelled live.


def age_connection_check(check, now_ms):
    """Deadline/countdown display only; a timeout is confirmed by the task owner."""
    result = copy.deepcopy(check)
    if type(now_ms) is not int:
        return result
    deadline = result.get('deadline_at_ms')
    if type(deadline) is int:
        result['deadline_elapsed'] = now_ms >= deadline
        result['deadline_in_seconds'] = max(0, (deadline - now_ms) / 1000)
    retry = result.get('retry_at_ms')
    if type(retry) is int:
        result['retry_in_seconds'] = max(0, (retry - now_ms) / 1000)
    return result
