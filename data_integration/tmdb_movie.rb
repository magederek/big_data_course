require 'mongoid'
require 'json'
require 'pp'
require 'date'

Encoding.default_external = 'UTF-8'
Encoding.default_internal = 'UTF-8'

Mongoid.load!("./mongoid.yml", :tmdb)

class TmdbMovie
  include Mongoid::Document
  store_in database: 'mongoid'
  field :title, type: String
  field :year, type: Integer
  field :rating, type: Float
  field :directors, type: Array
  field :casts, type: Hash
  field :main_casts, type: Array
  field :total_time, type: Integer
  field :languages, type: Array
  field :alias, type: Array
  field :genre, type: Array
  field :writers, type: Array
  field :keywords, type: Array
  field :description, type: String
  field :status, type: String
# 257175
  def self.json_to_db num_of_final = 20000
    Mongoid.load!("./mongoid.yml", :tmdb)

    num_of_movies = 257175

    movie_json = []
    puts "Reading from JSON file..."
    File.open('E:\big_data\db_20160229-135941.json', 'r') do |movie_file|
      movie_file.readline
      for i in 0..num_of_movies
        movie_json << movie_file.readline.sub(/,$/, '')
      end
    end


    movies_hash_array = []
    num_of_movies.times do |i|
      movie_hash = JSON.parse(movie_json[i])
      movies_hash_array << movie_hash
    end

# sort the movies by rating and numbers of not-null fields
    puts "Sorting with rating of movie..."
    movies_hash_array.sort! do |movie1, movie2|
      if movie1['rating'].nil?
        rating1 = 0
      else
        rating1 = movie1['rating']
      end
      if movie2['rating'].nil?
        rating2 = 0
      else
        rating2 = movie2['rating']
      end
      rating2 <=> rating1
    end

    puts "Sorting with # of not-null fields..."
    movies_hash_array.sort! do |movie1, movie2|
      movies = [movie1, movie2]
      nil_count = []
      nil_count[0], nil_count[1] = 0, 0
      2.times do |i|
        nil_count[i] += 16 if movies[i]['title'].nil?
        nil_count[i] += 1 if movies[i]['overview'].nil?
        nil_count[i] += 1 if movies[i]['tagline'].nil?
        nil_count[i] += 2 if movies[i]['year'].nil?
        nil_count[i] += 1 if movies[i]['rating_hint'].nil?
        nil_count[i] += 1 if movies[i]['directors'].nil?
        nil_count[i] += 1 if movies[i]['writers'].nil?
        nil_count[i] += 3 if movies[i]['top_casts'].nil?
        nil_count[i] += 1 if movies[i]['genres'].nil?
        nil_count[i] += 1 if movies[i]['companies'].nil?
        nil_count[i] += 1 if movies[i]['status'].nil?
        nil_count[i] += 1 if movies[i]['runtime'].nil?
        nil_count[i] += 1 if movies[i]['keywords'].nil?
      end
      nil_count[0] <=> nil_count[1]
    end

    movies_hash_array = movies_hash_array[0, num_of_final]

    # prepare for the language converting table
    lang_conv = JSON.parse(File.read('./languages_conv_table.json'))

    puts "Converting to TmdbMovie.class..."
    movies = []
    num_of_final.times do |i|
      puts "Current Converting #: #{i}" if i % 5000 == 0 && i != 0
      movie = TmdbMovie.new
      movie.title = movies_hash_array[i]["title"]
      movie.year = movies_hash_array[i]["year"]
      movie.rating = movies_hash_array[i]["rating_hint"]
      movie.directors = movies_hash_array[i]["directors"].split(', ') unless movies_hash_array[i]["directors"].nil?
      movie.casts = movies_hash_array[i]["casts"]
      movie.main_casts = movies_hash_array[i]["top_casts"]
      movie.total_time = movies_hash_array[i]["runtime"]
      if movies_hash_array[i]["languages"] != nil
        languages = Array.new
        movies_hash_array[i]["languages"].each do |language|
          languages << lang_conv[language]
        end
        movie.languages = languages
      elsif movies_hash_array[i]["language"] != nil
        movie.languages = [lang_conv[movies_hash_array[i]["language"]]]
      end
      movie.alias = movies_hash_array[i]["alternative_titles"]
      movie.genre = movies_hash_array[i]["genres"]
      movie.writers = movies_hash_array[i]["writers"]
      movie.keywords = movies_hash_array[i]["keywords"]
      movie.description = movies_hash_array[i]["overview"]
      movie.status = movies_hash_array[i]["status"]
      movies << movie
    end

    languages = []
    puts "Saving to tmdb_movie collection..."
    TmdbMovie.delete_all
    num_of_final.times do |i|
      unless movies[i].save
        puts "False: #{i}"
      end
      puts "Current Saving #: #{i}" if i % 5000 == 0 && i != 0
    end
  end

  def to_fused_movie
  end
end

# TmdbMovie.json_to_db


