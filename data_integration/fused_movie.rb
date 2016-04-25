require 'mongoid'
require 'pp'
require 'date'
require_relative '../algorithms/jaccard_array'
require_relative '../algorithms/jaccard_n_grams'
require_relative '../algorithms/monge_elkan'
require_relative './tmdb_movie'
require_relative './wiki_film'
require_relative './imdb_query'

Mongoid.load!("./mongoid.yml", :fused)

class FusedMovie
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
  field :country, type: Array
  field :genre, type: Array
  field :writers, type: Array
  field :filming_locations, type: Array
  field :keywords, type: Array

  @@client = Mongo::Client.new(['127.0.0.1:27018'], :database => 'movieactor', :monitoring => false)

  def self.parse_tmdb_movie movie
    if movie.class != TmdbMovie
      return nil
    else
      fused_movie = FusedMovie.new
      fused_movie.title = movie.title
      fused_movie.year = movie.year
      fused_movie.rating = movie.rating
      fused_movie.directors = movie.directors
      fused_movie.casts = movie.casts
      fused_movie.main_casts = movie.main_casts
      fused_movie.total_time = movie.total_time
      fused_movie.languages = movie.languages
      fused_movie.alias = movie.alias
      fused_movie.genre = movie.genre
      fused_movie.writers = movie.writers
      fused_movie.keywords = movie.keywords
      return fused_movie
    end
  end

  def self.parse_wiki_movie movie
    if movie.class != WikiFilm
      return nil
    else
      fused_movie = FusedMovie.new
      fused_movie.title = movie.title
      fused_movie.year = movie.year
      fused_movie.directors = movie.directors
      fused_movie.main_casts = movie.starring
      fused_movie.total_time = movie.total_time
      fused_movie.languages = movie.languages
      fused_movie.writers = movie.writers
      fused_movie.country = movie.country
      return fused_movie
    end
  end

  # load from tmdb collection by Array of _id
  # ids: Array of _id (String).
  def self.read_tmdb_by_ids
  end

  # load from tmdb collection of all items
  def self.read_tmdb
  end

  # load from imdb collection by a single _id
  # meanwhile, do data cleaning to remove non-movie with high propobility
  # and change some invalid hash key to be valid.
  # realize using mongo Ruby Driver 2.2
  def self.read_imdb_by_id id
    doc = @@client[:movie].find(:_id => BSON::ObjectId(id)).first
    fused_movie = FusedMovie.new
    fused_movie.title = doc['Title']
    fused_movie.year = doc['Year']
    fused_movie.rating = doc['Rating']
    fused_movie.directors = doc['Directors']
    casts = Hash.new
    if doc['Role'] != nil && doc['Main Cast'] != nil && doc['Role'].length == doc['Main Cast'].length
      i = 0
      doc['Role'].each do |role|
        casts.merge!({role => doc['Main Cast'].values[i]})
        i += 1
      end
    end
    fused_movie.casts = casts
    fused_movie.main_casts = doc['Role']
    fused_movie.total_time = doc['Total Time']
    fused_movie.languages = doc['Language']
    fused_movie.genre = doc['Genre']
    fused_movie.writers = doc['Writers']
    return fused_movie
  end

  # load from imdb collection by Array of _id
  # ids: Array of _id (String).
  def self.read_imdb_by_ids ids
    fused_movies = []
    ids.each do |id|
      fused_movies << (read_imdb_by_id id)
    end
    return fused_movies
  end

  # load from imdb collection of all items
  def self.read_imdb
  end

  def similarity other
    # initialize the weight of each field, the sum is 1
    title_w, year_w, directors_w, main_casts_w = 0.4, 0.15, 0.15, 0.15
    # similarity of title
    if !self.title.nil? && !other.title.nil?
      title_sim = JaccardNGrams.bigrams_sim self.title, other.title
    else
      title_sim = 0.0 # if some name is missing, they must be different
    end
    # similarity of year
    if !self.year.nil? && !other.year.nil?
      case self.year - other.year
        when 0 then year_sim = 1.0
        when 1 then year_sim = 0.2
        when -1 then year_sim = 0.2
        else year_sim = 0.0
      end
    else
      year_w = year_w / 10 # lower the weight of birthday
      year_sim = 0.0
    end
    # similarity of directors
    if !self.directors.nil? && !other.directors.nil?
      if self.directors.length >= other.directors.length
        directors_sim = MongeElkan.name_array_sim self.directors, other.directors
      else
        directors_sim = MongeElkan.name_array_sim other.directors, self.directors
      end
    else
      directors_w = directors_w / 10 # lower the weight of birthday
      directors_sim = 0.0
    end
    # similarity of main_casts
    if !self.main_casts.nil? && !other.main_casts.nil?
      if self.main_casts.length >= other.main_casts.length
        main_casts_sim = MongeElkan.name_array_sim self.main_casts, other.main_casts
      else
        main_casts_sim = MongeElkan.name_array_sim other.main_casts, self.main_casts
      end
    else
      main_casts_w = main_casts_w / 10 # lower the weight of birthday
      main_casts_sim = 0.0
    end
    # normalize the weights
    total_w = title_w + year_w + directors_w + main_casts_w
    title_w /= total_w
    year_w /= total_w
    directors_w /= total_w
    main_casts_w /= total_w

    # strict the similarity of title if all other attributes are nil
    title_sim = JaccardNGrams.trigrams_sim self.title, other.title if title_w > 0.85

    # compute the total similarity
    title_w * title_sim + year_w * year_sim + directors_w * directors_sim + main_casts_w * main_casts_sim
  end

  def match? other, threshold = 0.85
    # similarity of name
    if !self.title.nil? && !other.title.nil?
      title_sim = JaccardNGrams.bigrams_sim self.title, other.title
    else
      title_sim = 0.0 # if some title is missing, they must be different
    end
    # the title similarity is too low, they have very high Pr to be different
    if title_sim < threshold
      return false
    end
    # similarity of birthday
    if !self.year.nil? && !other.year.nil?
      return false if (self.year - other.year).abs > 1
      year_sim = (self.year == other.year ? 1.0 : 0.0)
    else
      year_sim = nil
    end
    # if both name and birthday similarity is very high, then they match
    if title_sim > (1 + threshold) / 2  && year_sim == 1.0
      true
    else
      similarity(other) >= threshold
    end
  end

  def self.group_by_first_char array
    groups = Hash.new
    array.each do |elem, key|
      elem.title.length.times do |i|
        if elem.title.slice(i) =~ /[A-Za-z0-9]/
          key = elem.title.slice(i).downcase
          break
        end
      end
      groups[key] ||= []
      groups[key] << elem
    end
    return groups
  end
end