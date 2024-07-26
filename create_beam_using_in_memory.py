# here in this example we can either use lambda or our customised functions in our beam
# pipeline.



import apache_beam as beam


def filter_list(element):
    if element>=30:
        return element
    else:
        pass
a=[10,30,40,50]
with beam.Pipeline() as pipe:
    ip_coll=(pipe
             |beam.Create(a)
             |beam.Filter(filter_list)
             |beam.Map(print)
             )