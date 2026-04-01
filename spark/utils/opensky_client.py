#this my version of the openskyclient api (you could use the public api https://openskynetwork.github.io/opensky-api/)
import requests
import logging
from datetime import datetime, timezone
from time import sleep

log = logging.getLogger(__name__)

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
                    auth = (self.username, self.password) if self.username else None,
                    params=params,
                    timeout=30
            )

            # Raise error for bad HTTP status
            response.raise_for_status()
            return response.json()

        except requests.exceptions.RequestException as e:
            logging.error(f"fetch_states failed: {e}")
            return {}
        except ValueError as e:
            # JSON decoding error
            logging.error(f"Invalid JSON response: {e}")
            return {}
        
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
                logging.warning(f"Skipping state with insufficient fields: {len(state)}")
                continue

            state_dict = {}
            state_dict['icao24'] = state[0]
            state_dict['callsign'] = state[1].strip() if state[1] else None
            state_dict['origin_country'] = state[2]
            state_dict['time_position'] = state[3]
            state_dict['last_contact'] = state[4]
            state_dict['longitude'] = float(state[5]) if state[5] is not None else None
            state_dict['latitude'] = float(state[6]) if state[6] is not None else None
            state_dict['baro_altitude'] = float(state[7]) if state[7] is not None else None
            state_dict['on_ground'] = state[8]
            state_dict['velocity'] = float(state[9]) if state[9] is not None else None
            state_dict['true_track'] = float(state[10]) if state[10] is not None else None
            state_dict['vertical_rate'] = float(state[11]) if state[11] is not None else None
            state_dict['sensors'] = [int(state[12])] if state[12] is not None and not isinstance(state[12], list) else (state[12] if state[12] is None else [int(s) for s in state[12]])
            state_dict['geo_altitude'] = float(state[13]) if state[13] is not None else None
            state_dict['squawk'] = state[14]
            state_dict['spi'] = state[15]
            state_dict['position_source'] = state[16]
            state_dict['category'] = state[17]


            state_dict['api_timestamp'] = time
            state_dict['ingested_at'] = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

            parsed.append(state_dict)

        return parsed
    
    def poll_forever(self, interval_seconds=10, lamin=None, lamax=None, lomin=None, lomax=None):
        while True:
            raw  = self.fetch_states(lamin=lamin, lamax=lamax, lomin=lomin, lomax=lomax)
            parsed = self.parse_states(data=raw)
            yield parsed
            sleep(interval_seconds)