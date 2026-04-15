from spark.utils.geo_utils import haversine_distance_py, nearest_airport_py


def test_haversine_distance_py():
    """distance between Charles de Gaulle and Heathrow"""
    CDG_lat, CDG_lon = 49.0097, 2.5478
    Heathrow_lat, Heathrow_lon = 51.4775, -0.4614

    d = haversine_distance_py( CDG_lat, CDG_lon, Heathrow_lat, Heathrow_lon)

    expected = 347.9

    assert abs(d - expected) < 1.0

def test_haversine_none_return():
    """returns None when any input is None"""
    CDG_lon =  2.5478
    Heathrow_lat, Heathrow_lon = 51.4775, -0.4614

    d = haversine_distance_py(None, CDG_lon, Heathrow_lat, Heathrow_lon)

    expected = None

    assert expected == d

def test_nearest_airport_none_input():

    RAK_lat = 31.6069

    airport = nearest_airport_py(RAK_lat, None)

    assert airport is None

def test_nearest_airport_within_range():
    """should return the airport icao passing marrakech airport"""
    RAK_lat, RAK_lon = 31.6069, -8.0363

    airport = nearest_airport_py(RAK_lat, RAK_lon)

    expected = "GMMX"

    assert expected == airport

def test_nearest_airport_outside_range():
    """check when there no airport in radius of 50km"""
    arwass_lat, arwass_lon = 26.241408, -7.900925

    airport = nearest_airport_py(arwass_lat, arwass_lon)

    assert airport is None



