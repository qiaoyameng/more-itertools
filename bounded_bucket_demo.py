#!/usr/bin/env python3
"""Demonstration for ``more_itertools.bounded_bucket``.

Two branches consume the same bounded bucket at different speeds. It shows:

* data belonging to the slow branch is cached, never discarded, while the
  fast branch scans for its next record;
* ``WOULD_BLOCK`` is a recoverable signal: after a key is closed (or another
  branch drains data), the blocked read resumes where it left off;
* every source record is delivered to exactly one branch, exactly once;
* iterators already handed out for a closed key terminate normally.
"""

from itertools import count

from more_itertools import AGAIN, EXHAUSTED, WOULD_BLOCK, bounded_bucket


def source():
    # A burst nobody wants long-term would overflow a capacity-3 bucket;
    # afterwards keys 'f' and 's' alternate.
    for i in range(6):
        yield ('q', i)
    # Three records for the slow branch fill the cache while the fast branch
    # is scanning; draining them must unblock the fast branch.
    for i in range(3):
        yield ('s', i)
    for i in range(10):
        yield ('f', i)
        yield ('s', i + 3)


def run():
    bucket = bounded_bucket(source(), key=lambda item: item[0], capacity=3)

    received = {'f': [], 's': []}
    blocks = {'f': 0, 's': 0}
    q_closed = False

    budget = {'f': 2, 's': 1}  # fast branch receives twice as many records

    def step(branch):
        """One micro-step: 'item', 'again', 'block', or 'exhausted'."""
        nonlocal q_closed
        result = bucket.advance(branch)
        if result is AGAIN:
            return 'again'  # another key's record was integrated
        if result is WOULD_BLOCK:
            blocks[branch] += 1
            if not q_closed and 'q' in bucket.keys():
                bucket.close('q')
                q_closed = True
                return 'closed_q'
            return 'block'
        if result is EXHAUSTED:
            return 'exhausted'
        received[branch].append(result)
        return 'item:' + repr(result)

    print('Fast branch receives 2 records/tick; the slow branch 1 every 2.')
    tick = 0
    done = set()
    while len(done) < 2:
        # Slow branch moves on even ticks and goes first, so a record it
        # drains frees capacity for the fast branch in the same round.
        order = ('s', 'f') if tick % 2 == 0 else ('f',)
        summary = []
        for branch in order:
            if branch in done:
                continue
            received_this_branch = 0
            notes = []
            for _ in range(budget[branch] + 3):  # cap AGAIN micro-steps
                if received_this_branch >= budget[branch]:
                    break
                outcome = step(branch)
                if outcome == 'exhausted':
                    done.add(branch)
                    notes.append('EXHAUSTED')
                    break
                if outcome.startswith('item:'):
                    received_this_branch += 1
                    notes.append('got ' + outcome.split(':', 1)[1])
                elif outcome == 'closed_q':
                    notes.append('blocked -> closed q')
                    break  # capacity freed; resume the read next round
                elif outcome == 'block':
                    notes.append('blocked (retry next round)')
                    break  # let the other branch free capacity first
            summary.append('%s: %s' % (branch, ', '.join(notes) or 'idle'))

        print('tick %2d retained=%d/%d keys=%r | %s'
              % (tick, len(bucket), bucket.capacity, bucket.keys(),
                 ' | '.join(summary)))

        tick += 1
        assert tick < 200, 'scheduler failed to finish'

    all_items = received['f'] + received['s']
    expected = [('f', i) for i in range(10)] + [('s', i) for i in range(13)]
    assert sorted(all_items) == sorted(expected), 'missing records'
    assert len(all_items) == len(set(all_items)), 'record delivered twice'
    assert blocks['f'] + blocks['s'] >= 1, 'back-pressure never fired'
    assert len(bucket) == 0, 'data leaked after drain: %d' % len(bucket)
    print()
    print('blocks observed: fast=%d slow=%d' % (blocks['f'], blocks['s']))
    print('fast got %d records, slow got %d records, each exactly once.'
          % (len(received['f']), len(received['s'])))

    other = bounded_bucket(
        [('k', 0), ('k', 1)], key=lambda item: item[0], capacity=2
    )
    iterator = other['k']
    print()
    print('outstanding iterator before close: %r' % (iterator.advance(),))
    other.close('k')
    print('after close(k), next advance: %r' % (iterator.advance(),))
    assert iterator.advance() is EXHAUSTED

    infinite = bounded_bucket(count(), key=lambda x: x % 2, capacity=4)
    assert infinite.keys() == []  # must not drain the infinite source
    assert 0 not in infinite
    print()
    print('keys()/contains stayed bounded on an infinite source.')
    print('All invariants hold.')


if __name__ == '__main__':
    run()
