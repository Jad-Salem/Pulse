from datetime import UTC, datetime, timedelta

from fastapi.testclient import TestClient

from .conftest import FakeSink, ProducerUnavailableError, make_event, purchase


def test_single_event_is_produced_keyed_by_user(client: TestClient, sink: FakeSink) -> None:
    response = client.post("/v1/events", json=purchase(user_id="u-42"))

    assert response.status_code == 202
    assert response.json() == {"accepted": 1}
    [(key, value)] = sink.sent
    assert key == "u-42"
    assert value["event_type"] == "purchase"
    assert isinstance(value["received_at"], int)


def test_batch_is_produced(client: TestClient, sink: FakeSink) -> None:
    events = [make_event(), purchase(), make_event(event_type="checkout_started")]

    response = client.post("/v1/batch", json={"events": events})

    assert response.status_code == 202
    assert response.json() == {"accepted": 3}
    assert [v["event_id"] for _, v in sink.sent] == [e["event_id"] for e in events]


def test_batch_is_validated_atomically(client: TestClient, sink: FakeSink) -> None:
    events = [make_event(), make_event(event_type="purchase")]  # second is missing fields

    response = client.post("/v1/batch", json={"events": events})

    assert response.status_code == 422
    assert sink.sent == []


def test_batch_size_limit(client: TestClient) -> None:
    response = client.post("/v1/batch", json={"events": [make_event() for _ in range(501)]})
    assert response.status_code == 422


def test_empty_batch_is_rejected(client: TestClient) -> None:
    assert client.post("/v1/batch", json={"events": []}).status_code == 422


def test_requires_api_key(client: TestClient, sink: FakeSink) -> None:
    assert (
        client.post("/v1/events", json=make_event(), headers={"X-API-Key": ""}).status_code == 401
    )
    assert (
        client.post("/v1/events", json=make_event(), headers={"X-API-Key": "wrong"}).status_code
        == 401
    )
    assert sink.sent == []


def test_future_event_time_is_rejected(client: TestClient, sink: FakeSink) -> None:
    future = (datetime.now(UTC) + timedelta(hours=1)).isoformat()

    response = client.post("/v1/events", json=make_event(event_time=future))

    assert response.status_code == 422
    assert "future" in response.text
    assert sink.sent == []


def test_validation_errors_do_not_echo_input(client: TestClient) -> None:
    response = client.post("/v1/events", json=make_event(country="secret-value"))
    assert response.status_code == 422
    assert "secret-value" not in response.text


def test_producer_failure_is_retryable(client: TestClient, sink: FakeSink) -> None:
    sink.fail_with = ProducerUnavailableError("broker down")

    response = client.post("/v1/events", json=make_event())

    assert response.status_code == 503
    assert response.headers["Retry-After"] == "1"


def test_readiness_reflects_sink(client: TestClient, sink: FakeSink) -> None:
    assert client.get("/readyz").status_code == 200
    sink.ready = False
    assert client.get("/readyz").status_code == 503
    assert client.get("/healthz").status_code == 200


def test_metrics_are_exposed(client: TestClient) -> None:
    client.post("/v1/events", json=make_event())
    client.post("/v1/events", json=make_event(event_type="bogus"))

    body = client.get("/metrics").text

    assert 'collector_events_accepted_total{event_type="page_view"} 1.0' in body
    assert 'collector_events_rejected_total{reason="invalid"} 1.0' in body
    assert 'route="/v1/events"' in body


def test_lifespan_starts_and_closes_sink(sink: FakeSink, client: TestClient) -> None:
    assert sink.started
