"""Developer utility: Optional Prune + OpenRouter gateway smoke test.

Security notice:
- Reads the key strictly from the PRUNE_API_KEY environment variable.
- Never prints, logs, or persists credentials.
- The PS-39 CRDT application operates entirely offline without any external services.
"""

import json
import os
import sys
import urllib.error
import urllib.request


ENDPOINT = "https://api.withprune.com/v1/proxy/openrouter/chat/completions"
MODEL = "openai/gpt-4o-mini"


def run_smoke_test() -> int:
    api_key = os.environ.get("PRUNE_API_KEY", "").strip()
    if not api_key:
        print("[STATUS: UNVERIFIED]")
        print("Reason: PRUNE_API_KEY environment variable is not set.")
        print("Note: The PS-39 replicated store is fully self-contained and operates")
        print("completely offline without external services. If you wish to verify the")
        print("Prune gateway separately, set PRUNE_API_KEY in your terminal session and rerun.")
        return 0

    headers = {
        "Content-Type": "application/json",
        "X-Prune-Key": api_key,
    }

    body = {
        "model": MODEL,
        "messages": [
            {"role": "user", "content": "Ping: Verify connection"}
        ],
        "max_tokens": 5,
    }

    data = json.dumps(body).encode("utf-8")
    req = urllib.request.Request(ENDPOINT, data=data, headers=headers, method="POST")

    print(f"Connecting to Prune gateway endpoint: {ENDPOINT}")
    print(f"Target model: {MODEL}")

    try:
        with urllib.request.urlopen(req, timeout=15) as response:
            status = response.status
            response_body = response.read().decode("utf-8")
            parsed = json.loads(response_body)

            print(f"[STATUS: VERIFIED] HTTP Status {status}")
            if "id" in parsed:
                print(f"Response ID: {parsed.get('id')}")
            if "model" in parsed:
                print(f"Returned Model: {parsed.get('model')}")
            if "choices" in parsed and len(parsed["choices"]) > 0:
                print("Gateway smoke test succeeded.")
            return 0
    except urllib.error.HTTPError as err:
        print(f"[STATUS: FAILED] HTTP Error {err.code}: {err.reason}")
        return 1
    except urllib.error.URLError as err:
        print(f"[STATUS: FAILED] Network Error: {err.reason}")
        return 1
    except Exception as exc:
        print(f"[STATUS: FAILED] Unexpected Error: {type(exc).__name__}")
        return 1


if __name__ == "__main__":
    sys.exit(run_smoke_test())
