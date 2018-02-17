import output
import datetime
import time
import numpy as np

import influxdb

# dewpoint constants (see https://en.wikipedia.org/wiki/Dew_point)
b = 17.27
c = 237.7

def gamma( Tc, RH ):
    """ gamma function takes temperature in celcius and percent relative humidity """
    return ( (np.log(RH/100.0)) + (b * Tc)/(c + Tc) )

def dewpoint ( Tc, RH ):
    """ calculate dewpoint in Celcius given temperature in celcius and relative humidity """
    return ( ( c * gamma(Tc, RH)) / (b - gamma(Tc, RH)) )


class InfluxDBWriter(output.Output):
    requiredSpecificParams = ["host", "port"]

    def __init__(self, config):
        super(InfluxDBWriter, self).__init__(config)

        self.host = self.params["host"]
        self.port = self.params["port"]

    def output_metadata(self, metadata):
        """Output metadata.

        Output metadata for the run in the format stipulated by this plugin.
        Metadata is set in airpi.py and then passed as a dict to each plugin
        which wants to output it.

        Args:
            self: self.
            metadata: dict The metadata for the run.

        """
        return True

    def output_data(self,dataPoints, sampletime):
        """Output data.

        Output data in the format stipulated by the plugin. Calibration is
        carried out first if required.
        Note this method takes account of the different data formats for
        'standard' sensors as distinct from the GPS. The former present a dict
        containing one value and associated properties such as units and
        symbols, while the latter presents a dict containing several readings
        such as latitude, longitude and altitude, but no units or symbols.

        Args:
            self: self.
            dataPoints: A dict containing the data to be output.

        """
        if self.params["calibration"]:
            datapoints = self.cal.calibrate(datapoints)

        client = influxdb.InfluxDBClient(host=self.host, port=self.port, database='airpi')

        tags = { 'hostname' : self.gethostname(),
                 'location' : 'basement',
                 'station' : 'MyAirPiBoard' }

        stamp = datetime.datetime.utcnow().isoformat()

        fields = {}
        for point in dataPoints:
            if "Relative_Humidity" in point["name"] and point["value"] > 100:
                continue
            if point["name"] != "Location":
                fields[point["sensor"]] = point["value"]

	fields["DHT22-dew"] = dewpoint( fields["DHT22-temp"], fields["DHT22-hum"])
        d = { "measurement": "record",
              "tags": tags,
              "time": stamp,
              "fields": fields }

        client.write_points([d])
        
        return True

