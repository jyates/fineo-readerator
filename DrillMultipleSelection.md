  * Drill projection logic is convoluted. Basically, we end up with:
	     00-01      ProjectAllowDup(*=[$0], field1=[$1], *0=[$2]) : rowType = RecordType(ANY *, ANY field1, ANY *0): rowcount = 1.0, cumulative cost = {18.0 rows, 45.08955000865387 cpu, 0.0 io, 0.0 network, 144.0 memory}, id = 708 
	     00-02        Project(*=[$0], field1=[$3], *0=[$0]) : rowType = RecordType(ANY *, ANY field1, ANY *0): rowcount = 1.0, cumulative cost = {18.0 rows, 45.08955000865387 cpu, 0.0 io, 0.0 network, 144.0 memory}, id = 707	     
  which means the Dup's second * is ```[expr=*0, ref=*0]```, which doesn't evaluate to anything. This is usually managed by a prefix, i.e. ```T0¦¦*```, but that isn't workable because we subsume that lower down in the "projection under union" logic from ```FinalColumnReorderer```.
  	* things that have been tried, but didn't work
  		* fixing schema from the recombinator: you can't expand the top projection, the row type won't match the original and gets rejected by the rules engine
  		* ignoring the extra projection via a Drill patch to ```FinalColumnReorderer```: you still end up with the same expansion element above the second, third, etc. union, which causes the same expansion of fields ```(*, c1, c2) => (*, c1, c2, c10, c20)```. 
  		* injecting a custom projection that fixes the schema above the FRR: keeps the ```T0¦¦``` argument for proper expansion in the scan, but still limited by the lack of expanding the * parameter
  	* Options for later:
  		* StarOverFixedExpression helper in the final physical plan generation
  			* Drill patch. Any * over a fixed schema would be expanded into the correct field names. Duplicate names would also be expanded.
  		* Union over views
  			* Rewrite query completely before execution to leverage views	
