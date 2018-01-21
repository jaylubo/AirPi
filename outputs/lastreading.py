import output
import datetime
import time

class LastReading(output.Output):
    requiredSpecificParams = ["outputDir", "outputFile"]

    def __init__(self, config):
        super(LastReading, self).__init__(config)
        if "<date>" in self.params["outputFile"]:
            filenamedate = time.strftime("%Y%m%d-%H%M")
            self.params["outputFile"] = self.params["outputFile"].replace("<date>", filenamedate)
        if "<hostname>" in self.params["outputFile"]:
            self.params["outputFile"] = self.params["outputFile"].replace("<hostname>", self.gethostname())
        # open the file persistently for write
        self.filename = self.params["outputDir"] + "/" + self.params["outputFile"]
        # write a header line so we know which sensor is which?
        self.header = False;

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

        line = "\"" + str(datetime.datetime.now()) + "\"," + str(time.time())
        header = "\"Date and time\",\"Unix time\""
        for point in dataPoints:
            if "Relative_Humidity" in point["name"] and point["value"] > 100:
                continue
            if point["name"] != "Location":
                header = "%s,\"%s %s (%s)\"" % (header, point["sensor"], point["name"], point["symbol"])
                line += "," + str(point["value"])
            else:
                header = "%s,\"Latitude (deg)\",\"Longitude (deg)\",\"Altitude (m)\",\"Exposure\",\"Disposition\"" % (header)
                props=["latitude", "longitude", "altitude", "exposure", "disposition"]
                for prop in props:
                    line += "," + str(point[prop])
        line = line[:-1]

        with open(self.filename, "w") as file:
            # write the header line
            file.write(header + "\n")
            # write the data line to the file
            file.write(line + "\n")

        return True

