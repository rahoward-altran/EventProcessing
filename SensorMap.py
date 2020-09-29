from typing import Dict, List

class SensorMap() :
    # the_map : Dict[str : Dict[str: List[int]]]
    # map = Dict[id : Dict[time : List[values]]
    def __init__(self, location_info):
        self.the_map = {}
        for location in location_info:
            self.the_map.setdefault(location['id'], {})

