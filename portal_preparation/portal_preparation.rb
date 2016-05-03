require_relative '../data_models/fused_actor'
require_relative '../data_models/fused_movie'
require 'date'

class PortalPreparation
  def self.popular_actors(limits: 300)
    Mongoid.load!('../data_models/mongoid.yml', :fused)
    popular_actors = FusedActor.all.sort do |a, b|
      ka = a.known_credits.nil? ? 0 : a.known_credits
      kb = b.known_credits.nil? ? 0 : b.known_credits
      kb <=> ka
    end
    popular_actors.reject! do |actor|
      birthday = actor.birthday.nil? ? Date.parse('2000-1-1'): actor.birthday
      birthday < Date.parse('1956-1-1')
    end
    popular_actors[0, limits].each do |actor|
      puts "#{actor.name}: #{actor.known_credits}"
    end
  end

  def self.popular_movie(genre: 'all', year: 'all', limits: 20)
    Mongoid.load!('../data_models/mongoid.yml', :fused)
    if genre == 'all'
      if year == 'all'
        popular_movies = FusedMovie.all.sort do |a, b|
          ra = a.rating.nil? ? 0 : a.rating
          rb = b.rating.nil? ? 0 : b.rating
          rb <=> ra
        end
      else
        popular_movies = FusedMovie.where(year: year).sort do |a, b|
          ra = a.rating.nil? ? 0 : a.rating
          rb = b.rating.nil? ? 0 : b.rating
          rb <=> ra
        end
      end
    else
      genre_regexp = Regexp.new(genre, true)
      if year == 'all'
        popular_movies = FusedMovie.where(genre: genre_regexp).sort do |a, b|
          ra = a.rating.nil? ? 0 : a.rating
          rb = b.rating.nil? ? 0 : b.rating
          rb <=> ra
        end
      else
        popular_movies = FusedMovie.where(year: year).where(genre: genre_regexp).sort do |a, b|
          ra = a.rating.nil? ? 0 : a.rating
          rb = b.rating.nil? ? 0 : b.rating
          rb <=> ra
        end
      end
    end

    popular_movies.reject! do |movie|
      movie.rating.nil? || movie.rating == 10.0
    end

    popular_movies[0, limits].each do |movie|
      puts "#{movie.title}: #{movie.rating} | year: #{movie.year}, genre: #{movie.genre.inspect}"
    end
  end

  def self.prepare_crew
    Mongoid.load!('../data_models/mongoid.yml', :fused)
  end

  def self.find_same_name_actor
    Mongoid.load!('../data_models/mongoid.yml', :fused)
    actors = FusedActor.all
    actor_hash = Hash.new
    actors.each do |actor|
      name = actor.name.nil? ? nil : actor.name
      actor_hash[name] ||= []
      match_id = actor.match_id.nil? ? -1 : actor.match_id
      actor_hash[name] << match_id
    end
    actor_hash.each_pair do |name, match_id|
      if match_id.uniq.length != 1
        name_regexp = Regexp.new("^#{name}$", true)
        conflict_actors = FusedActor.where({name: name_regexp})
        conflict_actors.each do |actor|
          puts actor.inspect
        end
        puts
      end
    end
  end

  def self.find_same_title_movie
    Mongoid.load!('../data_models/mongoid.yml', :fused)
    movies = FusedMovie.all
    movie_hash = Hash.new
    movies.each do |movie|
      title = movie.title.nil? ? nil : movie.title
      movie_hash[title] ||= []
      match_id = movie.match_id.nil? ? -1 : movie.match_id
      movie_hash[title] << match_id
    end
    movie_hash.each_pair do |title, match_id|
      if match_id.uniq.length != 1
        title_regexp = Regexp.new("^#{title}$", true)
        conflict_movies = FusedMovie.where({title: title_regexp})
        conflict_movies.each do |movie|
          puts movie.inspect
        end
        puts
      end
    end
  end

  def self.match_diff_actor_name
    Mongoid.load!('../data_models/mongoid.yml', :before_fused)
    data_fusion.linked_actor.each do |match_actors|
      if match_actors.length > 2 && (match_actors[0].name.downcase != match_actors[1].name.downcase)
        match_actors.each do |actor|
          puts "#{actor.match_id}: #{actor.inspect}"
        end
        puts
      end
    end
=begin
  if match_actors.length > 2 && (match_actors[0].name != match_actors[1].name || match_actors[0].name != match_actors[2].name || match_actors[1].name != match_actors[2].name)
    match_actors.each do |actor|
      puts "#{actor.match_id}: #{actor.name}"
    end
    puts
  end
=end
  end

  def self.match_diff_movie_title
    Mongoid.load!('../data_models/mongoid.yml', :before_fused)
    data_fusion.linked_movie.each do |match_movies|
      if match_movies.length > 1 && (match_movies[0].title.downcase != match_movies[1].title.downcase)
        match_movies.each do |movie|
          puts "#{movie.match_id}: #{movie.inspect}"
        end
      puts
      end
    end
=begin
  if match_movies.length > 2 && (match_movies[0].title.downcase != match_movies[1].title.downcase || match_movies[0].title.downcase != match_movies[2].title.downcase || match_movies[1].title.downcase != match_movies[2].title.downcase)
    match_movies.each do |movie|
      puts "#{movie.match_id}: #{movie.title}"
    end
    puts
  end
=end
  end
end



# PortalPreparation.popular_movie year: 2015, genre: 'comedy'
# PortalPreparation.find_same_name_actor
# PortalPreparation.find_same_title_movie