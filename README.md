# scoredb

A simple database optimized for returning results based on arbitrary combinations of fields.


# Why?

ScoreDB is optimized for systems that want to find the top scoring results, where the scoring function is specified by the client, and may depend on more than one field.  It may be a good choice for any system that needs to incorporate multiple factors when returning results.  For instance, it might power a used car website to produce results based on factors like milage, year, color, price, and distance.

# How?

ScoreDB uses a format on disk that is very similar to that used by text search systems like solr and elasticsearch.
We divide each field into ranges of values (buckets) and, for each bucket, maintain a file containing the ids of objects that have their value inside that range.
The ids in each file are strictly increasing; this means that we can traverse several buckets efficiently by using a heap of buckets to find the next smallest id among many buckets.
As we traverse the buckets, we score the objects produced and put them into a candidate result set.  The result set is capped at the limit specified by the user.  As poorly scoring results get kicked out of the candidate result set, we can infer a lower bound on the final score.  With some math, we can propagate that lower bound backwards through the scoring function to infer bounds on the individual fields.  These bounds may then be used to stop traversing very poorly scoring buckets that could not produce a good enough final score.  In this manner, as the candidate result set gets better and better, the system can eliminate more and more buckets to arrive at a result very quickly.
Note that several factors can influence the effectiveness of this approach: combining fields with addition, multiplication, and min() allow us to infer useful bounds.  Combining fields with a max() function does not, because a bad value in one field can be completely overcome by a good value in another.  Also, combining many fields instead of just a few can drastically change the rate of elimination, making the query take longer.

![Graph of bucket elimination during execution](bucket_execution.png)

# Run It

Scoredb has a straightforward programatic interface, but you can also run a standalone HTTP server like so:

> $ scoredb serve -datadir delmedir -port 11625
> $ curl -XPOST http://localhost:11625/index -d '{"age":38, "weight":190}'
> $ curl -G 'http://localhost:11625/query' --data-urlencode 'score=["sum", ["field", "age"], ["field", "weight"]]'

# Limitations

ScoreDB is minimalistic and highly specialized; it is intended to just act as one piece of a larger system:
* It stores objects as a flat set of key-value pairs with string keys and numeric values only. (internally, all values are 32 bit floating point values)
* ScoreDB can only respond to queries with lists of identifiers; ScoreDB's indexes do not provide efficient access to the original fields.
* ScoreDB requires the client to remember ScoreDB's identifier for each object; user-specified identifiers are not supported.
* ScoreDB has no built-in clustering, redundancy, or backup functions.
* ScoreDB has no delete or update operation.  To remove or change an object, you must build a new index.
* Adding objects to ScoreDB is slow if you add them one at a time.  Bulk insertion should be used whenever possible.
* ScoreDB requires many open files; sometimes thousands of them.  You will need to increase default filehandle limits on your system (see "ulimit" on linux).
* ScoreDB expects you to provide every field for every object; objects that are missing a field cannot be returned from queries that use the missing fields.

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
