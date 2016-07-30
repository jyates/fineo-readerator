# Readerator

Read data from various storage backend storage engines and serve the results to customers.

## Tests

Tests are run in two phases:
 
 1. Simple tests
  * as parallel as possible with multiple forked JVMS
 2. Cluster tests
  * annotated with ```@Category(ClusterTest.class)```
  * run serially and in their in JVMs
  
If you just want to run a cluster test independently, you can use the ```-Piso``` flag to enable 
those tests in a similar forked mode. Currently, ```iso``` is only supported in
```fineo-adapater-drill```, but it is merely a matter of copy-pasting that profile in another 
module. The same profile is also available in the parent, but because of the pecularities of 
Maven, child projects don't inherit profiles.
