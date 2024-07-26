# CombinePerKey():
# CombinePerKey is a function which expects a function as an argument like sum,min,max agg what else
# and even we can also pass our customised functions to CombinePerKey() function which is not possible in countPerKEy().

import apache_beam as beam

def parse_csv_line(line):
    return line.split(',')

def filter_data(element):
    return element[2] == "C"

def format_for_combination(element):
    return (element[2], int(float(element[6])))

def combine_max(values):
    return sum(values)

with beam.Pipeline() as pipe:
    ip_coll = (
        pipe
        | beam.io.ReadFromText("C:/Users/kesha/Downloads/wc_forecasts.csv")
        | beam.Map(parse_csv_line)
        | beam.Filter(filter_data)
        | beam.Map(format_for_combination)
        | beam.CombinePerKey(combine_max)
        | beam.Map(print)
    )
