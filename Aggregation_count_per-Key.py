# cont_Per_key:
# # count per key is a function which doesnt take any function as an input.
# here count per key will retrn how many records per key in file
# whereas if we use count Globally it will give how many total records are there in file or pcollection we can say.



import apache_beam as beam
with beam.Pipeline() as pipe:
    ip_coll=(pipe
             |beam.io.ReadFromText("C:/Users/kesha/Downloads/wc_forecasts.csv")
             |beam.Map(lambda x:x.split(","))
             |beam.Filter(lambda x:x[2]=="C")
             |beam.Map(lambda x:(x[1]+","+x[2],x))
             |beam.combiners.Count.PerKey()
             |beam.Map(print)
             )