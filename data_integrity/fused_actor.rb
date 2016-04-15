require 'mongoid'
require 'pp'
require 'date'
require_relative './tmdb_actor'

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

  def similarity a, b, threshold = 0.95

  end
end

#FusedActor.from_tmdb
puts TmdbActor.count
puts FusedActor.count

Mongoid.load!("./mongoid.yml", :fused)
#a = FusedActor.new('Jack', Date.new, 'M', 'US', 100, false, '2000', [], {father: 'Tom'}, 'Bio', [])

actor1 = TmdbActor.first
a = FusedActor.parse_tmdb_actor actor1
puts a.class
pp a.inspect
a.save




#Mongoid.load!("./mongoid.yml", :imdb)


#Mongoid.load!("./mongoid.yml", :fused)
