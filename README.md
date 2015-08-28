### Summary

Modules in this repository provide support for:

* collecting, parsing, and loading NYC geographic, object-relational data
* santizing, normalizing, parsing, and geocoding street addresses
* using a combination of numerous methods, cross-referencing and linking datasets:
	- [NYC tax lots and records](http://www.nyc.gov/html/dcp/html/bytes/applbyte.shtml#pluto)
	- [NYC restaurants on yelp.com and seamless.com](https://github.com/sethc23/seamless_yelp_scraping)
	- [NYC restaurant inspection results](https://data.cityofnewyork.us/Health/DOHMH-New-York-City-Restaurant-Inspection-Results/xx67-kt59)
* creating and maintaining up-to-date geographically-linked datasets
* spatial analysis, feature extraction, and some modeling

Most development initially took place on a PostgreSQL server where the data resides, and these modules merely served as paste boards for purposes of redundancy.  As a result, much of the early code lacks structure, annotation, and general-purpose utility, i.e., second half of `f_postgres.py`.  Until these functions can be culled and checked for dependencies, they will unfortunately remain part of this repository.

A database dump comprising all the tables affected by this project will become available in the near future. This database is about 500mb and exceeds the 100mb file size limit on github.com.

While not every function will be addressed, there are several noteworthy illustrations that will follow:

1. geographic object rendering, feature extraction, and cross-referencing
2. lattice implementation
3. automatic updating via triggers
4. fun example: turnstyles

