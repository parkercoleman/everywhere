#Off the Beatnik Path

###About
Off the Beatnik path is a "retro roadtrip planning" application that allows users to plan roadtrips using only roads that would have existed in the 1950s.

It accomplishes this by using TIGER road network data that has been hand edited to only include older roads (TODO: do this hand editing).  This means no third party APIs are used to calculate routes.

Currently Network NX is being for shortest route calculation, however, switching to Neo4j is on my short list.


###Running Locally
To run OTBP locally, you'll need python3 and postgres installed.

The python requirements can be installed with 

```
pip3 install -r /vagrant/requirements.txt
```

You'll probably need to install libgeos, check for documentation depending on your platform.

To setup the database, and any tables that don't rely on TIGER data, run

```
psql -f sql/table_setup.sql -h localhost -U postgres -W 
```

Next, you'll need the TIGER shapefiles, use manage.py to retrieve these:

```
python3 manage.py download
```

Next, we'll need to import some of this shapefile data into our database. 

```
python3 manage.py import --fips 22 28
```
The --fips argument here takes a list of FIPS ids if we want to limit the amount of data imported, this is advised for development, as the graph building process is currently time consuming.

Now we need some metadata tables created, these tables keep track of the locations of road intersections and selects the best intersection that should "represent" a city

```
psql -f sql/metadata_setup.sql -h localhost -U postgres -W otbp
```

Finally we're ready to create the graph, this is done by:

```
python3 manage.py create_graph
```

Once the graph is created the web application can be started with

```
python3 manage.py run
```
And can be accessed at http://localhost:5000/index.html

###Running Vagrant
(TODO)


