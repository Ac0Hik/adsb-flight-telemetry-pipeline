import requests
from unittest.mock import patch, MagicMock
from spark.utils.opensky_client import OpenSkyClient



@patch("spark.utils.opensky_client.requests.get")
def test_fetch_states_success(mock_get):
    mock_get.return_value = MagicMock(
        json=lambda: {"time": 123, "states": []},
        raise_for_status=lambda: None
    )

    client = OpenSkyClient()
    result = client.fetch_states()

    assert result == {"time": 123, "states": []}


@patch("spark.utils.opensky_client.requests.get")
def test_fetch_states_failure(mock_get):
    mock_get.side_effect = requests.exceptions.RequestException()

    client = OpenSkyClient()
    result = client.fetch_states()

    assert result == {}


def test_parse_states_returns_correct_fields():
    fake_state = [
        "abc123",      # icao24
        "BAW101   ",   # callsign (with trailing spaces)
        "United Kingdom",  # origin_country
        1617235200,    # time_position
        1617235200,    # last_contact
        -0.4614,       # longitude
        51.4775,       # latitude
        10000.0,       # baro_altitude
        False,         # on_ground
        250.0,         # velocity
        90.0,          # true_track
        0.0,           # vertical_rate
        None,          # sensors
        10500.0,       # geo_altitude
        "1234",        # squawk
        False,         # spi
        0,             # position_source
        0              # category
    ]

    fake_response = {"time": 123456, "states": [fake_state]}

    client = OpenSkyClient()
    result = client.parse_states(fake_response)

    assert len(result) == 1
    assert result[0]['callsign'] == "BAW101"
    assert result[0]['ingested_at'].endswith("Z")
    assert result[0]['api_timestamp'] == 123456

def test_parse_states_empty_states():
    fake_response = {"time": 123456, "states": None}

    client = OpenSkyClient()
    result = client.parse_states(fake_response)

    assert result == []


def test_poll_forever_yields_batches():
    fake_state = [
        "abc123", "BAW101   ", "United Kingdom",
        1617235200, 1617235200, -0.4614, 51.4775,
        10000.0, False, 250.0, 90.0, 0.0, None,
        10500.0, "1234", False, 0, 0
    ]

    fake_response = {"time": 123456, "states": [fake_state]}

    with patch("spark.utils.opensky_client.requests.get") as mock_get:
        mock_get.return_value = MagicMock(
            json=lambda: fake_response,
            raise_for_status=lambda: None
        )

        client = OpenSkyClient()
        gen = client.poll_forever(interval_seconds=0)

        batch = next(gen)

        assert isinstance(batch, list)
        assert len(batch) == 1
        assert batch[0]['icao24'] == "abc123"