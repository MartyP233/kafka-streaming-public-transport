"""Defines trends calculations for stations"""
from dataclasses import dataclass
import logging

import faust


logger = logging.getLogger(__name__)


# Faust will ingest records from Kafka in this format
@dataclass
class Station(faust.Record):
    stop_id: int
    direction_id: str
    stop_name: str
    station_name: str
    station_descriptive_name: str
    station_id: int
    order: int
    red: bool
    blue: bool
    green: bool


# Faust will produce records to Kafka in this format
@dataclass
class TransformedStation(faust.Record):
    station_id: int
    station_name: str
    order: int
    line: str


# TODO: Define a Faust Stream that ingests data from the Kafka Connect stations topic and
#   places it into a new topic with only the necessary information.
app = faust.App("stations-stream", broker="kafka://localhost:9092", store="memory://")
# TODO: Define the input Kafka Topic. Hint: What topic did Kafka Connect output to?
topic = app.topic("org.chicago.transit.stations", key_type=str, value_type=Station)
# TODO: Define the output Kafka Topic
out_topic = app.topic("org.chicago.transit.stations.transformed", partitions=1, key_type=str, value_type=TransformedStation)
# TODO: Define a Faust Table
table = app.Table(
   "stations_transformed",
   default=TransformedStation,
   partitions=1,
   changelog_topic=out_topic,
)

@app.agent(topic)
async def station(stations):
    async for station in stations:
        table[station.station_id] = TransformedStation(
            station_id= station.station_id,
            station_name=station.station_name,
            order=station.order,
            line= 'red' if station.red else 'blue' if station.blue else 'green' if station.green else ''
        )
        await out_topic.send(key=str(station.station_id), value=table[station.station_id])


#
#
# TODO: Using Faust, transform input `Station` records into `TransformedStation` records. Note that
# "line" is the color of the station. So if the `Station` record has the field `red` set to true,
# then you would set the `line` of the `TransformedStation` record to the string `"red"`
#
#


if __name__ == "__main__":
    app.main()
