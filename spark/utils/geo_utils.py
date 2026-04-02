from math import sin, cos, sqrt, asin, radians
from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType, StringType

airports = {
    # Europe
    "EGLL": (51.4775, -0.4614),       # London Heathrow
    "LFPG": (49.0097, 2.5478),        # Paris Charles de Gaulle
    "EBBR": (50.9010, 4.4844),        # Brussels Airport
    "LFBO": (43.6293, 1.3678),        # Toulouse Blagnac
    "EHAM": (52.3086, 4.7639),        # Amsterdam Schiphol
    "EDDF": (50.0379, 8.5622),        # Frankfurt
    "LEMD": (40.4936, -3.5668),       # Madrid Barajas
    "LIRF": (41.8003, 12.2389),       # Rome Fiumicino
    "LSZH": (47.4647, 8.5492),        # Zurich
    "EDDM": (48.3537, 11.7750),       # Munich
    "EKCH": (55.6181, 12.6561),       # Copenhagen
    "EPWA": (52.1657, 20.9671),       # Warsaw
    "LEBL": (41.2971, 2.0785),        # Barcelona
    "LFML": (43.4393, 5.2214),        # Marseille
    "LOWW": (48.1102, 16.5697),       # Vienna
    "UUEE": (55.9736, 37.4125),       # Moscow Sheremetyevo

    # Africa
    "GMMN": (33.3675, -7.5898),       # Casablanca Mohammed V
    "GMMX": (31.6069, -8.0363),       # Marrakech Menara
    "HECA": (30.1219, 31.4056),       # Cairo International
    "DNMM": (6.5774, 3.3216),         # Lagos Murtala Muhammed
    "HAAB": (8.9779, 38.7993),        # Addis Ababa Bole
    "FACT": (33.9648, 18.6017),       # Cape Town

    # North America
    "KJFK": (40.6413, -73.7781),      # New York JFK
    "KLAX": (33.9425, -118.4081),     # Los Angeles
    "CYYZ": (43.6772, -79.6306),      # Toronto Pearson
    "KMIA": (25.7959, -80.2870),      # Miami

    # Middle East & Asia
    "OMDB": (25.2532, 55.3657),       # Dubai
    "LLBG": (32.0114, 34.8867),       # Tel Aviv Ben Gurion
    "VHHH": (22.3080, 113.9185),      # Hong Kong
    "RJTT": (35.5494, 139.7798),      # Tokyo Haneda
    "WSSS": (1.3644, 103.9915),       # Singapore Changi
    "VIDP": (28.5562, 77.1000),       # Delhi Indira Gandhi
    "ZBAA": (40.0799, 116.6031),      # Beijing Capital
    "YSSY": (33.9399, 151.1753),      # Sydney Kingsford Smith
    "SBGR": (23.4356, -46.4731),      # São Paulo Guarulhos
}


def haversine_distance_py(lat1, lon1, lat2, lon2):
    if any(v is None for v in (lat1, lon1, lat2, lon2)):
        return None
    
    R = 6371 # constant in km 
    lat1, lon1 = radians(lat1), radians(lon1)
    lat2, lon2 = radians(lat2), radians(lon2)

    delta_lat = lat2 - lat1
    delta_lon = lon2 - lon1
    # a = sin²(Δlat/2) + cos(lat1) · cos(lat2) · sin²(Δlon/2)
    # c = 2 · arcsin(√a)
    # d = R . c
    return R*( 2*asin( sqrt( sin(delta_lat/2)**2  + cos(lat1)*cos(lat2)*(sin(delta_lon/2)**2)) ) )


def nearest_airport_py(lat, lon):
    if lat is None or lon is None:
        return None
    best_icao = None
    min_distance = float('inf')

    for airport_icao, coordinates in airports.items():
        airport_lat, airport_lon = coordinates[0], coordinates[1]
        d = haversine_distance_py(lat,lon, airport_lat, airport_lon)
        if d < min_distance:
            min_distance = d
            best_icao = airport_icao
    
    if min_distance <= 50:
        return best_icao
    return None


# register as pyspark udf
haversine_distance = udf(haversine_distance_py, DoubleType())
nearest_airport = udf(nearest_airport_py, StringType())
