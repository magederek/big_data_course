# Big Data Project - A Data Portal for Movies & Actor

##Brief Intorduction:
Source code of the big data course, including the crawling program, schema of database, portal page, etc.

##1. Folders and Files

####1. crawler
   1. ***tmdb_crawl_mt.rb***: The multi-thread version of crawl grabing the movie information from www.themoviedb.org without using API.
   2. ***tmdb_crawl_st_old.rb***: The original version which can grab only basic information. Not much useful...

####2. algorithms
   1. ***jaccard_n_grams.rb***: Use the algorithm of N-Grams combining with Jaccard Coefficient
   2. ***jaccard_array.rb***: Use Jaccard Coefficient to measure the similarity of two arrays; use Jaro-Winkler to determine the pairwise matching of each inside items.
   3. ***monge_elkan.rb***: The realization of Monge-Elkan algorithm. The inside similarity algorithms includes Jaccard (bigrams & trigrams) and Jaro-Winkler.
   4. ***sim_test.rb***: Use to test the similarity measuring methods.

####3. data_integration
   1. ***data_integration.rb***: Include the main function of data integration.
   2. ***tmdb_actor.rb***: Define the ODM class of the tmdb's actor database, including convert JSON file into a collection of MongoDB.
   3. ***fused_actor.rb***: Define the ODM of the fused actor database. Realize the data linkage and data fusion as its methods.
   3. ***mongoid.yml***: Define the configuration of mongoid gem used in TmdbActor and FusedActor.
   5. ***imdb_query.rb***: Return documents' _id according to the given year of actors or movies.

##2. Database Fields

##3. Project Procedure
####1. Data Acquisition
####2. Data Cleaning
####3. Entity Resolution
####4. Data Fusion
####5. Data Portal Coding

##4. Ruby Gems Used
1. [jaccard](https://rubygems.org/gems/jaccard): Calculate the jaccard Coefficient
2. [jaro_winkler](https://rubygems.org/gems/jaro_winkler): Jaro Winkler Algorithm realization
3. [json](https://rubygems.org/gems/json): Convert between Hash and JSON file
4. [nokogiri](https://rubygems.org/gems/nokogiri): Parse the HTML file to get information of tags)
5. [httparty](https://rubygems.org/gems/httparty): For downloading web page
6. [mongo](https://rubygems.org/gems/mongo): a Ruby Driver for MongoDB
7. [mongoid](https://rubygems.org/gems/mongoid): an ODM (Object Document Mapper) Framework for MongoDB

##5. References
