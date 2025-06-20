# Twitter-Data-Analysis

for now this code handle two simultaneous search each will take place in two
different terminal. We will be looking for a way to do the same once we will be
implementing the backEnd. 

### Brief summary of the code

when the user run it, he will specify the name of the query ("search request") 
that he want.

* first the system will check if there is an ongoing_search for the same request so that we will 
not consume another search engine
* second the system will check if there is any available search_engine at the moment, because twitter 
limit the number of request per 15 min per app auth user, so we had to engage 
one search engine for one request at a time.
* third it will proceed and create a table for the search request. The name of the query is 
cleaned from white spaces and ",".
* if we find  a search engine is available we will proceed with the request :
     *   we will get a sample of max result of 100 recent tweets per call
     *   after the end of each call we save the result to the DataWarehouse
     *   we repeat this process at least 400 times to gather as much info as we can get
* if a search appears to be empty => this means that we don't have any tweets about
  this subject for now we will pause the search for 5 minutes to let the timeline refresh
  and retry another time. We will put a limit on the number of tries per request
  meanwhile if there isn't any request that is waiting we can try as many times as we want,
  and for now we will put a limit of 5 tries per user.
* when we start the search for the request we will change the status of the search engine
and we will update the ongoing_Search table so we can keep track of the engine name and the query
that are being performed. In the end we will rearrange everything to the way it was just before
the start of the search.
     
     