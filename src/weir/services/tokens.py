"""Fast, dependency-free prompt token estimation used for *reservations*.

Reservations only need to be close: settlement always charges the provider's
reported usage. The heuristic (~4 chars/token for English, plus per-message
framing overhead) deliberately leans slightly high so reservations rarely
under-estimate. If `tiktoken` is installed it is used for OpenAI models.
"""

from __future__ import annotations

import math
from typing import Any

try:  # optional, exact counts for OpenAI tokenizers
    import tiktoken  # type: ignore

    _ENC = tiktoken.get_encoding("o200k_base")
except Exception:  # pragma: no cover - optional dependency
    _ENC = None

PER_MESSAGE_OVERHEAD = 4
PER_REQUEST_OVERHEAD = 3
IMAGE_TOKENS = 765  # conservative: one high-detail 512px tile set


def count_text(text: str) -> int:
    if not text:
        return 0
    if _ENC is not None:
        return len(_ENC.encode(text, disallowed_special=()))
    # ~4 chars/token, but never fewer than words (dense code/CJK skew this)
    return max(math.ceil(len(text) / 3.6), len(text.split()))


def _content_tokens(content: Any) -> int:
    if content is None:
        return 0
    if isinstance(content, str):
        return count_text(content)
    if isinstance(content, list):
        total = 0
        for part in content:
            if isinstance(part, dict):
                if part.get("type") in ("image_url", "input_image", "image"):
                    total += IMAGE_TOKENS
                else:
                    total += count_text(str(part.get("text", "")))
            else:
                total += count_text(str(part))
        return total
    return count_text(str(content))


def estimate_chat_prompt(body: dict) -> int:
    total = PER_REQUEST_OVERHEAD
    for msg in body.get("messages") or []:
        if not isinstance(msg, dict):
            continue
        total += PER_MESSAGE_OVERHEAD
        total += _content_tokens(msg.get("content"))
        if msg.get("name"):
            total += 1
        for call in msg.get("tool_calls") or []:
            total += count_text(str(call.get("function", {})))
    if body.get("tools"):
        total += count_text(str(body["tools"]))
    return total


def estimate_embedding_input(body: dict) -> int:
    inp = body.get("input")
    if isinstance(inp, str):
        return count_text(inp)
    if isinstance(inp, list):
        if inp and all(isinstance(x, int) for x in inp):
            return len(inp)  # pre-tokenised
        total = 0
        for item in inp:
            if isinstance(item, str):
                total += count_text(item)
            elif isinstance(item, list):
                total += len(item)
        return total
    return 0
