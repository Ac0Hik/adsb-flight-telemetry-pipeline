#this my version of the openskyclient api (you could use the public api https://openskynetwork.github.io/opensky-api/)
import requests
import logging
import os
from dotenv import load_dotenv
from datetime import datetime, timezone



class OpenSkyClient():
    def __init__(self,username =None,password=None):
        self.username = username
        self.password = password
        self.base_url = "https://opensky-network.org/api"

    def fetch_states(self, lamin=None, lamax=None, lomin=None, lomax=None):
        try:
            params = {}
            params["extended"] = 1
            if lamin is not None:
                params["lamin"] = lamin
            if lamax is not None:
                params["lamax"] = lamax
            if lomin is not None:
                params["lomin"] = lomin
            if lomax is not None:
                params["lomax"] = lomax
            response = requests.get(
                    f"{self.base_url}/states/all",
                    auth = (self.username, self.password),
                    params=params,
                    timeout=10
            )

            # Raise error for bad HTTP status
            response.raise_for_status()
            return response.json()

        except requests.exceptions.RequestException as e:
            logging.error(f"fetch_states failed: {e}")
            return None
        except ValueError as e:
            # JSON decoding error
            logging.error(f"Invalid JSON response: {e}")
            return None
        
    def parse_states(self, data=None):
        if data is None:
            return []
        time = data['time']
        states = data['states']
        if states is None:
            return []
        
        parsed = []
        #loop over aircrafts and create object append them to parsed
        for state in states:
            #skip responses that don't have all the fields
            if len(state) < 18:
                continue

            state_dict = {}
            state_dict['icao24'] = state[0]
            state_dict['callsign'] = state[1].strip() if state[1] else None
            state_dict['origin_country'] = state[2]
            state_dict['time_position'] = state[3]
            state_dict['last_contact'] = state[4]
            state_dict['longitude'] = state[5]
            state_dict['latitude'] = state[6]
            state_dict['baro_altitude'] = state[7]
            state_dict['on_ground'] = state[8]
            state_dict['velocity'] = state[9]
            state_dict['true_track'] = state[10]
            state_dict['vertical_rate'] = state[11]
            state_dict['sensors'] = state[12]
            state_dict['geo_altitude'] = state[13]
            state_dict['squawk'] = state[14]
            state_dict['spi'] = state[15]
            state_dict['position_source'] = state[16]
            state_dict['category'] = state[17]


            state_dict['api_timestamp'] = time
            state_dict['ingested_at'] = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


            parsed.append(state_dict)

        return parsed



load_dotenv()

opensky_user = os.getenv('OPENSKY_USER') if os.getenv('OPENSKY_USER') else None
opensky_pass = os.getenv('OPENSKY_PASS') if os.getenv('OPENSKY_PASS') else None

client = OpenSkyClient(username = opensky_user, password = opensky_pass)

res = client.fetch_states()
tada = client.parse_states(res)
print(tada[0])




        
