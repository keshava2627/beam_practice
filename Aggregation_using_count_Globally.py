import apache_beam as beam
with beam.Pipeline() as pipe:
    ip_coll=(pipe
             |beam.io.ReadFromText("C:/Users/kesha/Downloads/wc_forecasts.csv")
             |beam.Map(lambda x:x.split(","))
             |beam.Filter(lambda x:x[2]=="C")
             |beam.Map(lambda x:(x[1]+","+x[2],x))
             |beam.combiners.Count.Globally()
             |beam.Map(print)
             )