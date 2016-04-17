require 'mongoid'
require 'json'
require 'pp'
require 'date'
require 'nokogiri'
#require_relative './fused_actor'

Encoding.default_external = 'UTF-8'
Encoding.default_internal = 'UTF-8'

Mongoid.load!("./mongoid.yml", :tmdb)

class TmdbActor
  include Mongoid::Document
  store_in database: 'mongoid'
  field :name, type: String
  field :birthday, type: Date
  field :day_of_death, type: Date
  field :gender, type: String
  field :place_of_birth, type: String
  field :known_credits, type: Integer
  field :adult_actor, type: Boolean
  field :alias, type: Array
  field :biography, type: String
  field :known_for, type: Array
  field :acting, type: Hash
  
  def self.json_to_db nom_of_final = 20000
    Nokogiri::
    Mongoid.load!("./mongoid.yml", :tmdb)

    num_of_casts = 90202

    actor_json = []
    puts "Reading from JSON file..."
    File.open('E:\big_data\cast_json_2\acting_20160414-234602.json', 'r') do |cast_file|
      cast_file.readline
      for i in 0..num_of_casts
        actor_json << cast_file.readline.sub(/,$/, '')
      end
    end

#pp actor_json

    actors_hash_array = []
    num_of_casts.times do |i|
      actor_hash = JSON.parse(actor_json[i])
      #actor_hash.default = ''
      actors_hash_array << actor_hash
    end

# sort the actors by acting number & known_credits
    puts "Sorting with number of acting..."
    actors_hash_array.sort! do |actor1, actor2|
      if actor1["acting"].nil?
        length1 = 0
      else
        length1 = actor1["acting"].length
      end
      if actor2["acting"].nil?
        length2 = 0
      else
        length2 = actor2["acting"].length
      end
      length2 <=> length1
    end

    puts "Sorting with known_credits..."
    actors_hash_array.sort! do |actor1, actor2|
      if actor1["known_credits"].nil?
        credits1 = 0
      else
        credits1 = actor1["known_credits"].to_i
      end
      if actor2["known_credits"].nil?
        credits2 = 0
      else
        credits2 = actor2["known_credits"].to_i
      end
      credits2 <=> credits1
    end

    actors_hash_array = actors_hash_array[0, nom_of_final]

#pp actors_hash_array

    puts "Converting to Actor.class..."
    actors = []
    nom_of_final.times do |i|
      actor = TmdbActor.new
      actor.name = actors_hash_array[i]["full_name"]
      #unless actors_hash_array[i]["birthday"].nil?
      #  birth_array = actors_hash_array[i]["birthday"].split('-')
      #  actor.birthday = Date.new(birth_array[0].to_i, birth_array[1].to_i, birth_array[2].to_i) if birth_array.length == 3
      #end
      actor.birthday = actors_hash_array[i]["birthday"]
      #unless actors_hash_array[i]["day_of_death"].nil?
      #  day_of_death_array = actors_hash_array[i]["day_of_death"].split('-')
      #  actor.day_of_death = Date.new(day_of_death_array[0].to_i, day_of_death_array[1].to_i, day_of_death_array[2].to_i) if day_of_death_array.length == 3
      #end
      actor.day_of_death = actors_hash_array[i]["day_of_death"]
      case actors_hash_array[i]["gender"]
        when 'Female' then actor.gender = 'F'
        when 'Male' then actor.gender = 'M'
        else actor.gender = nil
      end
      unless actors_hash_array[i]["place_of_birth"].nil?
        if actors_hash_array[i]["place_of_birth"].length == 1
          actor.place_of_birth = actors_hash_array[i]["place_of_birth"][0].gsub(' - ', ', ')
        else
          actor.place_of_birth = actors_hash_array[i]["place_of_birth"].join(', ')
        end
      end
      actor.known_credits = actors_hash_array[i]["known_credits"]
      actor.adult_actor = actors_hash_array[i]["adult_actor"]
      actor.alias = actors_hash_array[i]["alternative_names"]
      actor.biography = actors_hash_array[i]["biography"]
      actor.known_for = actors_hash_array[i]["known_for"]
      acting = actors_hash_array[i]["acting"]
      acting.reject! do |movie, role|
        ((role.index /\(\d+ episodes?\)/ ) != nil) || role == 'Herself' || role == 'Himself'
      end
      actor.acting = acting
      actors << actor
    end

    puts "Saving to Actors collection..."
    TmdbActor.delete_all
    nom_of_final.times do |i|
      unless actors[i].save
        puts "False: #{i}"
      end
      puts "Current Actor #: #{i}" if i % 5000 == 0 && i != 0
    end
  end

  def to_fused_actor
  end
end

TmdbActor.json_to_db

