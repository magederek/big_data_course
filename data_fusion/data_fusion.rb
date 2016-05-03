require_relative '../data_models/fused_actor'
require_relative '../data_models/fused_movie'
require 'pp'
require 'json'

class DataFusion
  # @linked_actor: 2-D Array[][], the actors with the same match_id
  #                will be grouped in the same first dimension.
  # @linked_movie: 2-D Array[][], the movies with the same match_id
  #                will be grouped in the same first dimension.
  attr_accessor :linked_actor, :linked_movie, :source_weight


  def initialize
    Mongoid.load!('../data_models/mongoid.yml', :before_fused)
    @linked_actor = []
    @linked_movie = []
    @source_weight = Hash.new
    @source_weight['imdb'] = 0.36
    @source_weight['tmdb'] = 0.34
    @source_weight['wiki'] = 0.30

    for i in 0..(FusedActor.count - 1)
      if (data = FusedActor.where(match_id: i)) != []
        @linked_actor << data
      else
        break
      end
    end
    FusedActor.where(match_id: nil).each do |actor|
      @linked_actor << [actor]
    end
    for i in 0..(FusedMovie.count - 1)
    #for i in 0..1000
      if (data = FusedMovie.where(match_id: i)) != []
        @linked_movie << data
      else
        break
      end
    end
    FusedMovie.where(match_id: nil).each do |movie|
      @linked_movie << [movie]
    end
  end

  def fuse_actor
    Mongoid.load!('../data_models/mongoid.yml', :fused)
    FusedActor.delete_all
    puts "Fusing Actors ..."
    current_i = 0

    linked_actor.each do |actors|
      actor = FusedActor.new
      len_name = 0
      vote_birthday = Hash.new(0.0)
      vote_gender = Hash.new(0.0)
      vote_placeofbirth = Hash.new(0.0)
      vote_nationality = Hash.new(0.0)
      vote_knowncredits = Hash.new(0.0)
      vote_adultactor = Hash.new(0.0)
      vote_yearsactive = Hash.new(0.0)
      union_alias = []
      union_knownfor = []
      union_acting = []
      union_biography = ''
      match_id = nil
      db_names = ''

      actors.each do |actor_in_row|
        if actor_in_row.name.length > len_name
          len_name = actor_in_row.name.length
          actor.name = actor_in_row.name
        end

        weight = @source_weight[actor_in_row.db_name]

        vote_birthday[actor_in_row.birthday] += weight  unless actor_in_row.birthday.nil?
        vote_gender[actor_in_row.gender] += weight unless actor_in_row.gender.nil?
        vote_placeofbirth[actor_in_row.place_of_birth] += weight unless actor_in_row.place_of_birth.nil?
        vote_nationality[actor_in_row.nationality] += weight unless actor_in_row.nationality.nil?
        vote_knowncredits[actor_in_row.known_credits] += weight unless actor_in_row.known_credits.nil?
        vote_adultactor[actor_in_row.adult_actor] += weight unless actor_in_row.adult_actor.nil?
        vote_yearsactive[actor_in_row.years_active] += weight unless actor_in_row.years_active.nil?
        union_alias |= actor_in_row.alias unless actor_in_row.alias.nil?
        union_biography += (actor_in_row.biography + "\n") unless actor_in_row.biography.nil?
        union_acting |= actor_in_row.acting unless actor_in_row.acting.nil?
        union_knownfor |= actor_in_row.known_for unless actor_in_row.known_for.nil?
        match_id = actor_in_row.match_id
        db_names += "#{actor_in_row.db_name};"
      end

      actor.birthday = vote_birthday.key(vote_birthday.values.max)
      actor.gender = vote_gender.key(vote_gender.values.max)
      actor.place_of_birth = vote_placeofbirth.key(vote_placeofbirth.values.max)
      actor.nationality = vote_nationality.key(vote_nationality.values.max)
      actor.known_credits = vote_knowncredits.key(vote_knowncredits.values.max)
      actor.adult_actor = vote_adultactor.key(vote_adultactor.values.max)
      actor.years_active = vote_yearsactive.key(vote_yearsactive.values.max)
      actor.alias = union_alias
      actor.biography = union_biography
      actor.acting = union_acting
      actor.known_for = union_knownfor
      actor.match_id = match_id
      actor.db_name = db_names

      actor.save
      current_i += 1
      puts "Fusing Actor #{current_i} ..." if current_i % 1000 == 0
    end
  end

  def fuse_movie
    Mongoid.load!('../data_models/mongoid.yml', :fused)
    FusedMovie.delete_all
    puts "Fusing Actors ..."

    current_i = 0
    @linked_movie.each do |movies|
    #100.times do |i|
      #movies = linked_movie[i]
      fused_movie = FusedMovie.new
      vote_title = Hash.new(0.0)
      vote_year = Hash.new(0.0)
      vote_rating = Hash.new(0.0)
      merge_directors = []
      merge_casts = Hash.new
      merge_main_casts = []
      vote_total_time = Hash.new(0.0)
      merge_languages = []
      merge_alias = []
      merge_country = []
      merge_genre = []
      merge_writers = []
      merge_filming_locations = []
      merge_keywords = []
      match_id = nil
      db_names = ''
      movies.each do |movie|
        weight = @source_weight[movie.db_name]
        vote_title[movie.title] += weight if movie.title != nil
        vote_year[movie.year] += weight if movie.year != nil
        vote_rating[movie.rating] += weight if movie.rating != nil
        merge_directors |= movie.directors if movie.directors != nil
        merge_casts.merge! movie.casts if movie.casts != nil
        merge_main_casts |= movie.main_casts if movie.main_casts != nil
        vote_total_time[movie.total_time] += weight if movie.total_time != nil
        merge_languages |= movie.languages if movie.languages != nil
        merge_alias |= movie.alias if movie.alias != nil
        merge_country |= movie.country if movie.country != nil
        merge_genre |= movie.genre if movie.genre != nil
        merge_writers |= movie.writers if movie.writers != nil
        merge_filming_locations |= movie.filming_locations if movie.filming_locations != nil
        merge_keywords |= movie.keywords if movie.keywords != nil
        match_id = movie.match_id
        db_names += "#{movie.db_name};"
      end
      fused_movie.title = vote_title.key(vote_title.values.max)
      fused_movie.year = vote_year.key(vote_year.values.max)
      rating_result = 0
      rating_weight_sum = vote_rating.values.inject(0, :+)
      vote_rating.each_pair do |rating, vote|
        rating_result += rating * vote / rating_weight_sum
      end
      fused_movie.rating = rating_result
      fused_movie.directors = merge_directors
      fused_movie.casts = merge_casts
      fused_movie.main_casts = merge_main_casts
      fused_movie.total_time = vote_total_time.key(vote_total_time.values.max)
      fused_movie.languages = merge_languages
      fused_movie.alias = merge_alias
      fused_movie.country = merge_country
      fused_movie.genre = merge_genre
      fused_movie.writers = merge_writers
      fused_movie.filming_locations = merge_filming_locations
      fused_movie.keywords = merge_keywords
      fused_movie.match_id = match_id
      fused_movie.db_name = db_names
      fused_movie.save
      current_i += 1
      puts "Fusing Movie #{current_i} ..." if current_i % 1000 == 0
    end
  end

  def deduplicate_actor
    Mongoid.load!('../data_models/mongoid.yml', :fused)

  end
end

