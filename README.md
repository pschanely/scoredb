# scoredb

A simple database optimized for returning results based on arbitrary combinations of fields.


# Why?

ScoreDB is optimized for systems that want to find the top scoring results, where the scoring function is specified by the client, 
and may depend on more than one field.
It may be a good choice for any system that needs to incorporate multiple factors when returning results.
For instance, it might power a used car website to produce results based on factors like milage, year, and distance.

# Performance

Few database systems support custom scoring functions, and fewer (possibly none?) use algorithms designed for that purpose.
In practice, I've found elasticsearch's custom scoring functions to be quite fast, so I've benchmarked against it here.
(please let me know about other systems I might benchmark against!)

This is a graph of how 5 different queries perform with varying database sizes (yellow is elasticsearch and blue is scoredb):
<img src="scale_performance.png" width="300">
The elasticsearch query times (yellow) look like they're rising exponentially, but it's actually linear, on account of the logarithmic scale.

The dataset is anonymized US census data, each object representing an individual.  These are the 5 scoring functions used for benchmarking, in order from fastest to slowest (for scoredb):

```
10 * number_of_children + age
10000 * age + yearly_wages
100 * age + yearly_wages
40 * gender + weekly_work_hours
100.0 * gender + 9 * num_children + age + weekly_work_hours
5 * num_children + age + weekly_work_hours
```

It's clear from the graph that scoredb's performance can vary significantly based on the scoring function.
Read more about that below.

# Run It

Though Scoredb has a straightforward programatic interface, you can run a simple, standalone HTTP server like so:

```
$ scoredb serve -datadir delmedir -port 11625
... and in another shell:
$ # insert some people with ages and weights
$ curl -XPUT http://localhost:11625/jim -d '{"age":21, "weight":170}'
$ curl -XPUT http://localhost:11625/bob -d '{"age":34, "weight":150}'
$ # get people by age, weight, or the sum of their age and weight:
$ curl -G 'http://localhost:11625' --data-urlencode 'score=["field", "age"]'
$ curl -G 'http://localhost:11625' --data-urlencode 'score=["field", "weight"]'
$ curl -G 'http://localhost:11625' --data-urlencode 'score=["sum", ["field", "age"], ["field", "weight"]]'
```

# How?

ScoreDB uses a format on disk that is very similar to that used by text search systems like solr and elasticsearch.
We divide each field into ranges of values (buckets) and, for each bucket, maintain a file containing the ids of objects that have their value inside that range.

The ids in each file are strictly increasing; this means that we can traverse several buckets efficiently by using a heap of buckets to find the next smallest id among many buckets.

As we traverse the buckets, we score the objects produced and put them into a candidate result set.  The result set is capped at the limit specified by the user.  As poorly scoring results get kicked out of the candidate result set, we can infer a lower bound on the final score.  With some math, we can propagate that lower bound backwards through the scoring function to infer bounds on the individual fields.  These bounds may then be used to stop traversing very poorly scoring buckets that could not produce a good enough final score.  In this manner, as the candidate result set gets better and better, the system can eliminate more and more buckets to arrive at a result very quickly.

The following graph shows bucket elimination over the course of a query combining two fields, "age" and "wages":
<img src="bucket_execution.png" widht="300">

Because of the way ScoreDB works, some scoring functions will perform much better than others.  Some guidance:

* Prefer to combine fields with addition, multiplication, and, in particular, minimum, because they allow the computation of useful lower bounds.  Combining fields with a max() function does not, because a bad value in one field can be completely overcome by a good value in another.
* Combining many fields instead of a few will make the query take longer, because it takes longer to determine useful lower bounds on each field.
* Prefer to engineer weights so that the contributions from each of your fields is similar in scale.  ScoreDB may never be able to find useful bounds on fields that tweak the final score very slightly.


# Limitations

ScoreDB is minimalistic and highly specialized; it is intended to just act as one piece of a larger system:
* It stores objects as a flat set of key-value pairs with string keys and numeric values only. (internally, all values are 32 bit floating point values)
* ScoreDB can only respond to queries with lists of identifiers; ScoreDB's indexes do not provide efficient access to the original field data.
* ScoreDB has no built-in clustering, redundancy, or backup functions.
* ScoreDB has no delete or update operation.  To remove or change an object, you must build a new index.
* Adding objects to ScoreDB is slow if you add them one at a time.  Bulk insertion should be used whenever possible.
* ScoreDB requires many open files; sometimes thousands of them.  You will need to increase default filehandle limits on your system (see "ulimit" on linux).
* ScoreDB expects you to provide every field for every object; objects that are missing a field cannot be returned from queries that use the missing fields.
* ScoreDB data files are endian specific; most modern CPUs are little endian, so you won't normally have to worry about this.

# Thanks

Thanks are due to the [Samsung Accelerator](http://samsungaccelerator.com) which let us start this project as a hackathon proof of concept.  Scoredb was built with this awesome team (in github lexicographic order!):

* https://github.com/davidgljay
* https://github.com/ploxiln
* https://github.com/pschanely (Phil Schanely)
* https://github.com/rmarianski
* https://github.com/sleepylemur

# Plugs

Check out of some of our other side projects too:

* [wildflower-touch](https://github.com/pschanely/wildflower-touch) is proof-of-concept programming IDE and language for touch devices.
* [music-tonight](http://musictonightapp.com) makes playlists of bands playing near you, tonight.
