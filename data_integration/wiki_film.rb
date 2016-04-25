# Code programmed by JIN, Yue

require 'mongoid'
require 'json'
require 'pp'
require 'date'

Encoding.default_external = 'UTF-8'
Encoding.default_internal = 'UTF-8'


class WikiFilm
  include Mongoid::Document
  store_in database: 'wiki'
  field :title, type: String
  field :year, type: Integer
  field :directors, type: Array
  field :editors, type: Array
  field :starring, type: Array
  field :country, type: Array
  field :total_time, type: Integer
  field :languages, type: Array
  field :writers, type: Array
  field :producer, type: String
  field :production_company, type: Array
  field :budget, type: String
  field :box_office, type: String
  

  def self.json_to_db
    Mongoid.load!("./mongoid.yml", :wiki)
    film_json = []
    films_hash_array = []
    puts "Reading from JSON file..."

    j = 0
    path = "/Users/Crystal/Desktop/Data_Portal/6000D/film0"
    Dir.glob("#{path}/**/*.json").each do |f_path|       
      puts f_path
      film_json[j] = File.read(f_path)
      film_hash = JSON.parse(film_json[j]) 
      films_hash_array.push film_hash
      j += 1
    end

    puts "Converting to WikiFilm.class..."
    films = []
    len = films_hash_array.length
    puts len
    # num_of_final.times do |i|
    for i in 0..(len-1)

      film = WikiFilm.new

      film.title = films_hash_array[i]["title"]
      film.year = films_hash_array[i]["year"]

      unless films_hash_array[i]["directors"].nil?
          film.directors = films_hash_array[i]["directors"]
      end  
      unless films_hash_array[i]["editors"].nil?
          film.editors = films_hash_array[i]["editors"]
      end
      unless films_hash_array[i]["writers"].nil?
          film.writers = films_hash_array[i]["writers"]
      end
      unless films_hash_array[i]["starring"].nil?
          film.starring = films_hash_array[i]["starring"]
      end
      unless films_hash_array[i]["country"].nil?
          film.country = films_hash_array[i]["country"]
      end
      unless films_hash_array[i]["total_time"].nil?
          film.total_time = films_hash_array[i]["total_time"]
      end
      unless films_hash_array[i]["languages"].nil?
          film.languages = films_hash_array[i]["languages"]
      end
      unless films_hash_array[i]["producer"].nil?
          film.producer = films_hash_array[i]["producer"]
      end
      unless films_hash_array[i]["production_company"].nil?
          film.production_company = films_hash_array[i]["production_company"]
      end
      unless films_hash_array[i]["budget"].nil?
          film.budget = films_hash_array[i]["budget"]
      end
      unless films_hash_array[i]["box_office"].nil?
          film.box_office = films_hash_array[i]["box_office"]
      end  

      films.push film

    end

    # languages = []
    puts "Saving to wiki_movie collection..."
    WikiFilm.delete_all
    len = films.length
    len.times do |i|
      unless films[i].save
        puts "False: #{i}"
      end
      puts "Current Saving #: #{i}" if i % 5000 == 0 && i != 0
    end
  end

end

# WikiFilm.json_to_db
