require 'mongoid'
require 'pp'
require 'date'
require_relative '../algorithms/jaccard_array'
require_relative '../algorithms/jaccard_n_grams'
require_relative '../algorithms/monge_elkan'
require_relative './tmdb_movie'
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

  @@client = Mongo::Client.new(['127.0.0.1:27018'], :database => 'moviemovie', :monitoring => false)

  def self.parse_tmdb_movie movie
    if movie.class != Tmdbmovie
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
      doc['Role'].each do |role, i|
        casts.merge!({role => doc['Main Cast'].values[i]})
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
    n_w, b_w, g_w, pob_w, kf_w = 0.5, 0.12, 0.12, 0.14, 0.12
    # similarity of name
    if !self.name.nil? && !other.name.nil?
      if self.name.split(' ').length < other.name.split(' ').length
        name_sim = MongeElkan.jaro_winkler_sim(self.name, other.name)
      else
        name_sim = MongeElkan.jaro_winkler_sim(other.name, self.name)
      end
      # puts name_sim
    else
      name_sim = 0 # if some name is missing, they must be different
    end
    # similarity of birthday
    if !self.birthday.nil? && !other.birthday.nil?
      birthday_sim = (self.birthday == other.birthday ? 1.0 : 0)
    else
      b_w = b_w / 10 # lower the weight of birthday
      birthday_sim = 0
    end
    # similarity of gender
    if !self.gender.nil? && !other.gender.nil?
      gender_sim = (self.gender == other.gender ? 1.0 : 0)
    else
      g_w = g_w / 10 # lower the weight of birthday
      gender_sim = 0
    end
    # similarity of place_of_birth
    if !self.place_of_birth.nil? && !other.place_of_birth.nil?
      place_of_birth_sim = JaccardNGrams.trigrams_sim(self.place_of_birth, other.place_of_birth)
    else
      pob_w = pob_w / 10 # lower the weight of birthday
      place_of_birth_sim = 0
    end
    # similarity of known_for
    if !self.known_for.nil? && !other.known_for.nil?
      known_for_sim = JaccardArray.sim(self.known_for, other.known_for)
    else
      kf_w = kf_w / 10 # lower the weight of birthday
      known_for_sim = 0
    end
    # normalize the weights
    total_w = n_w + b_w + g_w + pob_w + kf_w
    n_w /= total_w
    b_w /= total_w
    g_w /= total_w
    pob_w /= total_w
    kf_w /= total_w
    # compute the total similarity
    n_w * name_sim + b_w * birthday_sim + g_w * gender_sim + pob_w * place_of_birth_sim + kf_w * known_for_sim
  end

  def match? other, threshold = 0.95
    # similarity of name
    if !self.name.nil? && !other.name.nil?
      if self.name.split(' ').length < other.name.split(' ').length
        name_sim = MongeElkan.jaro_winkler_sim(self.name, other.name)
      else
        name_sim = MongeElkan.jaro_winkler_sim(other.name, self.name)
      end
    else
      name_sim = 0 # if some name is missing, they must be different
    end
    # the name similarity is too low, they have very high Pr to be different
    if name_sim < 0.8
      return false
    end
    # similarity of birthday
    if !self.birthday.nil? && !other.birthday.nil?
      birthday_sim = (self.birthday == other.birthday ? 1.0 : 0)
    else
      birthday_sim = nil
    end
    # if both name and birthday similarity is very high, then they match
    if name_sim >= 0.90 && birthday_sim == 1.0
      true
    else
      similarity(other) >= threshold
    end
  end
end