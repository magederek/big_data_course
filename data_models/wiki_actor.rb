# Code programmed by JIN, Yue

require 'mongoid'
require 'json'
require 'pp'
require 'date'
require 'nokogiri'


Encoding.default_external = 'UTF-8'
Encoding.default_internal = 'UTF-8'

class WikiActor
  include Mongoid::Document
  store_in database: 'wiki'
  field :name, type: String
  field :alias, type: String
  field :birthday, type: Date
  field :place_of_birth, type: String 
  field :gender, type: String
  field :years_active, type: String
  field :education, type: Array  
  field :nationality, type: Array
  field :known_for, type: Array
  field :spouse, type: Array
  field :children, type: Integer
  field :partner, type: Array

  # field :acting, type: Hash
  def self.json_to_db
    Mongoid.load!('../data_models/mongoid.yml', :wiki)
    ### Read json file
    actor_json = []
    actors_hash_array = []
    puts "Reading from JSON file..."

    j = 0
    path = "/Users/Crystal/Desktop/Data_Portal/6000D/actor0"
    Dir.glob("#{path}/**/*.json").each do |a_path|       
      puts a_path
      actor_json[j] = File.read(a_path)
      actor_hash = JSON.parse(actor_json[j]) 
      actors_hash_array.push actor_hash
      j += 1
    end

    puts "Converting to WikiActor.class..."
    actors = []
    # i = 0
    len = actors_hash_array.length
    puts len
    # len.times do |i|
    for i in 0..(len-1)
      actor = WikiActor.new
      actor.name = actors_hash_array[i]["name"]

      unless actors_hash_array[i]["alias"].nil?
          actor.place_of_birth = actors_hash_array[i]["alias"]
      end
      actor.birthday = actors_hash_array[i]["birthday"]
      unless actors_hash_array[i]["place_of_birth"].nil?
          actor.place_of_birth = actors_hash_array[i]["place_of_birth"]
      end 
      actor.gender = actors_hash_array[i]["gender"]
      actor.years_active = actors_hash_array[i]["years_active"]
      unless actors_hash_array[i]["education"].nil?
          actor.education = actors_hash_array[i]["education"]
      end 
      actor.nationality = actors_hash_array[i]["nationality"]
      unless actors_hash_array[i]["known_for"].nil?
          actor.known_for = actors_hash_array[i]["known_for"]
      end     
      unless actors_hash_array[i]["spouse"].nil?
          actor.spouse = actors_hash_array[i]["spouse"]
      end
      unless actors_hash_array[i]["children"].nil?
          actor.children = actors_hash_array[i]["children"]
      end
      unless actors_hash_array[i]["partner"].nil?
          actor.partner = actors_hash_array[i]["partner"]
      end

      actors.push actor

    end

    puts "Saving to Actors collection..."
    WikiActor.delete_all
    len = actors.length
    puts len
    len.times do |i|
      unless actors[i].save
        puts "False: #{i}"
      end
      puts "Current Actor #: #{i}" if i % 5000 == 0 && i != 0
    end

  end

end

# WikiActor.json_to_db
