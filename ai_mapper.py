import json
import os
from typing import Any, Dict, List, Optional


def _has_openai_key() -> bool:
    key = os.getenv("OPENAI_API_KEY")
    print("DEBUG _has_openai_key:", bool(key))
    return bool(key)


def infer_column_mapping(
    *,
    side: str,
    columns: List[str],
    sample_rows: List[Dict[str, Any]],
    user_prompt: str,
    model: Optional[str] = None,
) -> Dict[str, str]:
    """Infer a logical-field -> column-name mapping using an LLM.

    Returns a dict like:
        {"mid": "Merchant Id", "tid": "Terminal", "card": "Pan", "amount": "Gross"}

    If OPENAI_API_KEY is not set (or OpenAI import fails), returns {} (caller should fallback).
    """

    if not _has_openai_key():
        return {}

    try:
        from openai import OpenAI  # type: ignore
    except Exception:
        return {}

    client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
    model = model or os.getenv("OPENAI_MODEL", "gpt-4o-mini")

    # Structured output schema (strict) so we always get predictable JSON.
    schema: Dict[str, Any] = {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "mapping": {
                "type": "object",
                "additionalProperties": False,
                "properties": {
                    "mid": {"type": "string"},
                    "tid": {"type": "string"},
                    "card": {"type": "string"},
                    "amount": {"type": "string"},
                },
                "required": ["mid", "tid", "card", "amount"],
            }
        },
        "required": ["mapping"],
    }

    system = (
        "You map spreadsheet columns to reconciliation fields. "
        "Return ONLY JSON that matches the provided schema. "
        "Choose column names EXACTLY from the provided list. "
        "If you are unsure, pick the best match anyway."
    )

    user = {
        "side": side,
        "available_columns": columns,
        "sample_rows": sample_rows,
        "user_prompt": user_prompt,
        "fields_needed": {
            "mid": "merchant ID / MID",
            "tid": "terminal ID / TID",
            "card": "card number (masked or full)",
            "amount": "transaction amount / gross sales",
        },
    }

    resp = client.responses.create(
        model=model,
        input=[
            {"role": "system", "content": system},
            {
                "role": "user",
                "content": "JSON mapping task. Input follows as JSON:\n" + json.dumps(user, ensure_ascii=False, default=str),
            },
        ],
        text={
            "format": {
                "type": "json_schema",
                "name": "reconciliation_mapping",
                "strict": True,
                "schema": schema,
            }
        },
    )

    raw = getattr(resp, "output_text", None) or ""
    try:
        data = json.loads(raw)
    except Exception:
        # Last resort: try to locate JSON in output.
        start = raw.find("{")
        end = raw.rfind("}")
        if start >= 0 and end > start:
            data = json.loads(raw[start : end + 1])
        else:
            return {}

    mapping = (data or {}).get("mapping") or {}
    # Ensure string keys.
    out: Dict[str, str] = {}
    for k in ("mid", "tid", "card", "amount"):
        v = mapping.get(k)
        if isinstance(v, str):
            out[k] = v
    return out
