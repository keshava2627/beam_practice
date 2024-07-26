# Here we are writing a simple beam code as a pipeline which will read csv file and split using "," as we
# know that split() function will form a list.so it will return a list as output.
#
# ->first we need to import apache_beam as beam
# ->we need to intialise the pipeline by using beam.Pipeline()
# and we need to read a csv file and it will store in a variable which is our
# PCollection.
# and in between we have PTransformation.
# and we have to make sure that we have started our pipeline as well by using pipe.run() method





import apache_beam as beam
pipe=beam.Pipeline()
input_coll=(pipe
            |beam.io.ReadFromText("C:/Users/kesha/Downloads/wc_forecasts.csv")
            |beam.Map(lambda x:x.split(","))
            |beam.Map(print)

            )
pipe.run()