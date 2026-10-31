import time

from gateway.core.circuit import BreakerState, CircuitBreaker
from gateway.services.proxy import SSEUsageTap
from gateway.services.tokens import estimate_chat_prompt, estimate_embedding_input


def test_sse_tap_handles_events_split_across_chunks():
    stream = (
        b'data: {"choices":[{"delta":{"content":"a"}}],"usage":null}\n\n'
        b'data: {"choices":[{"delta":{"content":"b"}}],"usage":null}\n\n'
        b'data: {"choices":[],"usage":{"prompt_tokens":11,"completion_tokens":2}}\n\n'
        b"data: [DONE]\n\n"
    )
    for size in (1, 3, 7, 64, len(stream)):
        tap = SSEUsageTap()
        for i in range(0, len(stream), size):
            tap.feed(stream[i : i + size])
        tap.close()
        assert tap.usage == (11, 2), size
        assert tap.events == 3


def test_sse_tap_without_usage_counts_events():
    tap = SSEUsageTap()
    tap.feed(b'data: {"choices":[{"delta":{"content":"x"}}]}\r\n\r\ndata: {"choices":[]}\n')
    tap.close()
    assert tap.usage is None
    assert tap.events == 2


def test_sse_tap_ignores_garbage():
    tap = SSEUsageTap()
    tap.feed(b': keepalive\n\nevent: ping\ndata: {"usage": {broken\n\n')
    assert tap.usage is None


def test_circuit_breaker_lifecycle():
    b = CircuitBreaker("t", failure_threshold=2, recovery_seconds=0.05)
    assert b.allow()
    b.record_failure()
    assert b.state is BreakerState.CLOSED
    b.record_failure()
    assert b.state is BreakerState.OPEN
    assert not b.allow()  # short-circuits instantly
    time.sleep(0.06)
    assert b.allow()  # half-open probe
    assert not b.allow()  # only one probe at a time
    b.record_failure()
    assert b.state is BreakerState.OPEN
    time.sleep(0.06)
    assert b.allow()
    b.record_success()
    assert b.state is BreakerState.CLOSED and b.allow()


def test_token_estimates_are_sane():
    small = estimate_chat_prompt({"messages": [{"role": "user", "content": "hi"}]})
    big = estimate_chat_prompt({"messages": [{"role": "user", "content": "word " * 1000}]})
    assert 4 <= small < 20
    assert 1000 <= big < 1600
    multimodal = estimate_chat_prompt({"messages": [{"role": "user", "content": [
        {"type": "text", "text": "what is this"}, {"type": "image_url", "image_url": {}}]}]})
    assert multimodal > 700
    assert estimate_embedding_input({"input": [1, 2, 3, 4]}) == 4
    assert estimate_embedding_input({"input": ["a b", "c"]}) >= 2
