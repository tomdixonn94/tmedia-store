import json
import os
import sys
from pathlib import Path
from typing import Any

import requests
from google.oauth2 import service_account
from google.auth.transport.requests import AuthorizedSession

STATE_PATH = Path("support-alerts/support-watch-state.json")
TOPIC = "tmedia_admin_support"
MAX_QUERY = 100
MAX_REMEMBERED = 500


def fail(message: str) -> None:
    print(f"ERROR: {message}", file=sys.stderr)
    raise SystemExit(1)


def firestore_value(value: dict[str, Any] | None) -> Any:
    if not value:
        return None
    for key in (
        "stringValue",
        "timestampValue",
        "integerValue",
        "doubleValue",
        "booleanValue",
    ):
        if key in value:
            return value[key]
    if "nullValue" in value:
        return None
    return value


def fields(document: dict[str, Any]) -> dict[str, Any]:
    return {
        key: firestore_value(value)
        for key, value in document.get("fields", {}).items()
    }


def load_state() -> dict[str, Any]:
    if not STATE_PATH.exists():
        return {"initialised": False, "processed": []}
    try:
        state = json.loads(STATE_PATH.read_text(encoding="utf-8"))
        state.setdefault("initialised", False)
        state.setdefault("processed", [])
        return state
    except Exception:
        return {"initialised": False, "processed": []}


def save_state(processed: list[str]) -> None:
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    data = {
        "initialised": True,
        "processed": processed[-MAX_REMEMBERED:],
    }
    STATE_PATH.write_text(
        json.dumps(data, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def main() -> None:
    raw_secret = os.environ.get("FIREBASE_SERVICE_ACCOUNT", "").strip()
    if not raw_secret:
        fail("GitHub secret FIREBASE_SERVICE_ACCOUNT is missing.")

    try:
        info = json.loads(raw_secret)
    except json.JSONDecodeError as exc:
        fail(f"FIREBASE_SERVICE_ACCOUNT is not valid JSON: {exc}")

    project_id = info.get("project_id")
    if not project_id:
        fail("Service-account JSON has no project_id.")

    credentials = service_account.Credentials.from_service_account_info(
        info,
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )
    session = AuthorizedSession(credentials)

    query_url = (
        f"https://firestore.googleapis.com/v1/projects/{project_id}"
        "/databases/(default)/documents:runQuery"
    )
    query = {
        "structuredQuery": {
            "from": [{"collectionId": "messages", "allDescendants": True}],
            "where": {
                "fieldFilter": {
                    "field": {"fieldPath": "sender"},
                    "op": "EQUAL",
                    "value": {"stringValue": "customer"},
                }
            },
            "orderBy": [
                {"field": {"fieldPath": "sentAt"}, "direction": "DESCENDING"}
            ],
            "limit": MAX_QUERY,
        }
    }

    response = session.post(query_url, json=query, timeout=45)
    if response.status_code >= 400:
        fail(f"Firestore query failed ({response.status_code}): {response.text}")

    rows = response.json()
    documents = [row["document"] for row in rows if "document" in row]

    state = load_state()
    processed = list(dict.fromkeys(state["processed"]))
    processed_set = set(processed)

    # First run creates a checkpoint without notifying for historical messages.
    if not state["initialised"]:
        save_state([doc["name"] for doc in reversed(documents)])
        print(
            f"Initial checkpoint created for {len(documents)} existing customer messages. "
            "No old alerts were sent."
        )
        return

    new_documents = [doc for doc in documents if doc["name"] not in processed_set]
    new_documents.reverse()  # Send oldest unseen message first.

    if not new_documents:
        print("No new customer support messages.")
        return

    fcm_url = (
        f"https://fcm.googleapis.com/v1/projects/{project_id}/messages:send"
    )

    sent_names: list[str] = []
    for document in new_documents:
        name = document["name"]
        values = fields(document)
        text = str(values.get("text") or "New customer support message")[:220]

        marker = "/documents/"
        relative = name.split(marker, 1)[1] if marker in name else ""
        parts = relative.split("/")
        if len(parts) < 4 or parts[0] != "conversations":
            print(f"Skipping unexpected message path: {name}")
            sent_names.append(name)
            continue

        conversation_id = parts[1]
        conversation_url = (
            f"https://firestore.googleapis.com/v1/projects/{project_id}"
            f"/databases/(default)/documents/conversations/{conversation_id}"
        )
        conversation_response = session.get(conversation_url, timeout=30)
        conversation = (
            fields(conversation_response.json())
            if conversation_response.status_code == 200
            else {}
        )

        support_code = str(conversation.get("supportCode") or "Customer")
        customer_token = str(conversation.get("fcmToken") or "")

        payload = {
            "message": {
                "topic": TOPIC,
                "notification": {
                    "title": f"New support message — {support_code}",
                    "body": text,
                },
                "data": {
                    "title": f"New support message — {support_code}",
                    "body": text,
                    "conversationId": conversation_id,
                    "supportCode": support_code,
                    "customerToken": customer_token,
                    "type": "support_message",
                },
                "android": {
                    "priority": "high",
                    "notification": {
                        "channel_id": "support_messages",
                        "sound": "default",
                    },
                },
            }
        }

        send_response = session.post(fcm_url, json=payload, timeout=30)
        if send_response.status_code >= 400:
            fail(
                f"FCM send failed for {conversation_id} "
                f"({send_response.status_code}): {send_response.text}"
            )

        print(f"Alert sent for {support_code}: {text}")
        sent_names.append(name)

    processed.extend(sent_names)
    processed = list(dict.fromkeys(processed))[-MAX_REMEMBERED:]
    save_state(processed)
    print(f"Sent {len(sent_names)} support alert(s).")


if __name__ == "__main__":
    main()
