## FAQ

### Why do some APIs have a query with a type `string` and others with a `map[string]interface{}`?

- The APIs with a string type do not need to modify the query logic after it comes in, whereas
the ones using `map[string]interface{}` need to modify the query logic.

### What the Hell is a PIT?

- A PIT is a "Point In Time". It is used to retrieve consistent results from the database, especially during pagination.
For example, if I am using a time based parameter to query using SearchAfter, I would not want the pages to change as I
am querying for the next page. The PIT is used to ensure that the results are consistent across pages.
See this page for more information: [Elasticsearch PIT](https://www.elastic.co/guide/en/elasticsearch/reference/current/point-in-time-api.html)

### What is the difference between the Queries and Mutations terms?

- This is something I'm experimenting with, since it's actually GraphQL terminology. In GraphQL, queries are used to 
retrieve data from the server, and mutations are used to modify data on the server. Thus DB queries that retrieve data
are housed under queries, and DB queries that modify data are housed under mutations.