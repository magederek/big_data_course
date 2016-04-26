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

####3. data_models
   1. ***tmdb_movie.rb, tmdb_actor.rb, wiki_film, wiki_actor, fused_movie, fused_actor***: ODM class of the collections of database, _Mongoid_ is used to realize them.
   2. ***imdb_query.rb***: Return documents' _id according to the given year of actors or movies.
   3. ***mongoid.yml***: Define the configuration of mongoid gem used in TmdbActor and FusedActor.
   4. ***languages_conv_table.json***: The key-value for converting abbreviation of languages into full ones.

####4. entity_resolution
   1. ***entity_resolution.rb***: Class with the methods for Entity Resolution.
   2. ***entity_resolution_exec.rb***: The execution of Entity Resolution.

####5. data_fusion
   1. ***data_fusion.rb***: Class with the methods for Data Fusion.
   2. ***data_fusion_exec.rb***: The execution of Entity Resolution.

##2. Project Procedure
####1. Data Acquisition
Use the cawlers to crawl data of movies and actors from <http://themoviedb.org/>, <http://www.imdb.com/>
 and <https://www.wikipedia.org/>. The original data are stored in files of JSON and XML format.

####2. Data Cleaning
Before the data are stored in the database, some cleaning and adjustment is performed to
remove the data unwanted and modify the format of the attribute-value. After then, the data
from different source are saved into different database with different schemas.

####3. Entity Resolution
One of the crucial steps is Entity Resolution which is to link different entries those refer to the
same entity in reality. We use blocking + pairwise matching to perform the data linkage. Several string
similarity algorithms are used in this stage, including Jaccard Coefficient, Jaro Winkler, etc.
At the end of the step, all of the entries are stored in the same database using the same schema.
Each entry with the linkage to other entry in different database will be assigned an _match_id_.
In other words, entries refer to the same entity will have the same _match_id_.

####4. Data Fusion
This step is to merge the entries with the same _match_id_. We use the methods of weighted voting,
longest string, etc., to finish data fusion. At the end of this stage, different entries with the
same _match_id_ are merge into one, and all of the data are saved into a separate final database.

####5. Data Portal Coding
We use lite Ruby web framework Rails/Sinatra to finish the web page of the data portal.

##3. Database Schema
####1. fused_movies
  * title: String
  * year: Integer
  * rating: Float
  * directors: Array
  * casts: Hash
  * main_casts: Array
  * total_time: Integer
  * languages: Array
  * alias: Array
  * country: Array
  * genre: Array
  * writers: Array
  * filming_locations: Array
  * keywords: Array
  * match_id: Integer
  * db_name: String

####2. fused_actors
  * name: String
  * birthday: Date
  * gender: String
  * place_of_birth: String
  * nationality: String
  * known_credits: Integer
  * adult_actor: Boolean
  * years_active: String
  * alias: Array
  * biography: String
  * known_for: Array
  * match_id: Integer
  * db_name: String

##4. Ruby Gems Used
   1. [jaccard](https://rubygems.org/gems/jaccard): Calculate the jaccard Coefficient
   2. [jaro_winkler](https://rubygems.org/gems/jaro_winkler): Jaro Winkler Algorithm realization
   3. [json](https://rubygems.org/gems/json): Convert between Hash and JSON file
   4. [nokogiri](https://rubygems.org/gems/nokogiri): Parse the HTML file to get information of tags)
   5. [httparty](https://rubygems.org/gems/httparty): For downloading web page
   6. [mongo](https://rubygems.org/gems/mongo): a Ruby Driver for MongoDB
   7. [mongoid](https://rubygems.org/gems/mongoid): an ODM (Object Document Mapper) Framework for MongoDB

##5. References
   1. ISO 639 Language Code List: <https://www.loc.gov/standards/iso639-2/php/code_list.php>
   2. Felix Naumannm, "Similarity measures" [DPDC_12_Similarity]
   3. JENS BLEIHOLDER and FELIX NAUMANN, "Data Fusion", _ACM Computing Surveys, Vol. 41, No. 1, Article 1_