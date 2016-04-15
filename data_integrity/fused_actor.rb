require 'mongoid'
require 'pp'
require 'date'
require_relative './tmdb_actor'
require_relative '../algorithms/jaccard_array'
require_relative '../algorithms/jaccard_n_grams'
require_relative '../algorithms/monge_elkan'

Mongoid.load!("./mongoid.yml", :fused)

class FusedActor
  include Mongoid::Document
  store_in database: 'movie_actor'
  field :name, type: String
  field :birthday, type: Date
  field :gender, type: String
  field :place_of_birth, type: String
  field :known_credits, type: Integer
  field :adult_actor, type: Boolean
  field :years_active, type: String
  field :alias, type: Array
  field :relative, type: Hash
  field :biography, type: String
  field :known_for, type: Array

  def self.parse_tmdb_actor actor
    if actor.class != TmdbActor
      return nil
    else
      fused_actor = FusedActor.new
      fused_actor.name = actor.name
      fused_actor.birthday = actor.birthday
      fused_actor.gender = actor.gender
      fused_actor.place_of_birth = actor.place_of_birth
      fused_actor.known_credits = actor.known_credits
      fused_actor.adult_actor = actor.adult_actor
      fused_actor.alias = actor.alias
      fused_actor.biography = actor.biography
      fused_actor.known_for = actor.known_for
      return fused_actor
    end
  end

  # load from tmdb collection
  def self.read_tmdb_database
    Mongoid.load!("./mongoid.yml", :tmdb)
    tmdb_actors = TmdbActor.count
    puts tmdb_actors
  end

  def similarity other, threshold = 0.95
    n_w, b_w, g_w, pob_w, kf_w = 0.5, 0.12, 0.12, 0.14, 0.12

    # similarity of name
    if !self.name.nil? && !other.name.nil?
      name_sim = MongeElkan.jaro_winkler_simavg(self.name, other.name)
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
    total_sim = n_w * name_sim + b_w * birthday_sim + g_w * gender_sim + pob_w * place_of_birth_sim + kf_w * known_for_sim
    puts total_sim
    total_sim > 0.95
  end
end

a1 = FusedActor.where(name: "Scarlett Johansson").first
a2 = FusedActor.first
a3 = FusedActor.where(name: "Scarlett Johansson").first
a3.name = "Scarlett Johnson"
a3.gender = nil
a3.birthday = nil

puts a1.similarity(a2)
puts a1.similarity(a3)