require_relative '../data_models/fused_actor'
require_relative '../data_models/fused_movie'
require 'pp'

class EntityResolution
  def self.pairwise_match_actor_odm actors1, actors2
    Mongoid.load!('../data_models/mongoid.yml', :before_fused)
    if (actors1.nil? || actors1.length == 0) && (actors2.nil? || actors2.length == 0)
      return []
    elsif actors1.nil? || actors1.length == 0
      matches = []
      actors2.each do |actor|
        matches << [actor]
      end
      return matches
    elsif actors2.nil? || actors2.length == 0
      matches = []
      actors1.each do |actor|
        matches << [actor]
      end
      return matches
    else
      matches = Array.new
      (actors1.length).times do |i|
        match = Array.new
        match_flag = false
        (actors2.length).times do |j|
          if actors1[i].match? actors2[j]
            match_flag = true
            if !actors1[i].match_id.nil?
              actors2[j].match_id = actors1[i].match_id
              actors2[j].save
            elsif !actors2[j].match_id.nil?
              actors1[i].match_id = actors2[j].match_id
              actors1[i].save
            elsif actors1[i].match_id.nil? && actors2[j].match_id.nil?
              actors1[i].match_id = FusedActor.current_match_id
              actors2[j].match_id = FusedActor.current_match_id
              actors1[i].save
              actors2[j].save
              FusedActor.current_match_id += 1
            end
            match << actors1[i]
            match << actors2[j]
            matches << match
            actors2.slice!(j)
            puts "#{i} match #{j}"
            break
          end
        end
        unless match_flag
          matches << [actors1[i]]
        end
        puts "Comparing actor #{i} ..." if i % 10 == 0
      end
      actors2.length.times do |i|
        matches << [actors2[i]]
      end
      return matches
    end
  end

  def self.pairwise_match_actor_groups array
    for i in 0..(array.length - 2)
      for j in (i + 1)..(array.length - 1)
        actor_key_compared = []
        puts "Begin to pairwise match actor #{array[i].values[0][0].db_name} & #{array[j].values[0][0].db_name}..."
        other = array[j].clone
        array[i].each_key do |key|
          #puts "================ Matching in Group \"#{key}\" =================="
          actor_key_compared << key
          pairwise_match_actor_odm(array[i][key], other[key])
        end
        other.each_key do |key|
          unless actor_key_compared.include? key
            print "#{key}, "
            pairwise_match_actor_odm(nil, other[key])
          end
        end
      end
    end
  end

  # input : ({db1 => movies2}, {db2 => movies2})
  def self.pairwise_match_movie_odm movies1, movies2
    #return nil if movie_hash1.class != Hash || movie_hash2.class != Hash
    if (movies1.nil? || movies1.length == 0) && (movies2.nil? || movies2.length == 0)
      return []
    elsif movies1.nil? || movies1.length == 0
      matches = []
      movies2.each do |movie|
        matches << [movie]
      end
      return matches
    elsif movies2.nil? || movies2.length == 0
      matches = []
      movies1.each do |movie|
        matches << [movie]
      end
      return matches
    else
      matches = Array.new
      (movies1.length).times do |i|
        match = Array.new
        match_flag = false
        (movies2.length).times do |j|
          if movies1[i].match? movies2[j]
            match_flag = true
            if !movies1[i].match_id.nil?
              movies2[j].match_id = movies1[i].match_id
              movies2[j].save
            elsif !movies2[j].match_id.nil?
              movies1[i].match_id = movies2[j].match_id
              movies1[i].save
            elsif movies1[i].match_id.nil? && movies2[j].match_id.nil?
              movies1[i].match_id = FusedMovie.current_match_id
              movies2[j].match_id = FusedMovie.current_match_id
              movies1[i].save
              movies2[j].save
              FusedMovie.current_match_id += 1
            end
            match << movies1[i]
            match << movies2[j]
            matches << match
            movies2.slice!(j)
            puts "#{i} match #{j}"
            break
          end
        end
        unless match_flag
          matches << [movies1[i]]
        end
        puts "Comparing movie #{i} ..." if i % 10 == 0
      end
      movies2.length.times do |i|
        matches << [movies2[i]]
      end
      return matches
    end
  end

  def self.pairwise_match_movie_groups array
    for i in 0..(array.length - 2)
      for j in (i + 1)..(array.length - 1)
        movie_key_compared = []
        puts "Begin to pairwise match movie #{array[i].values[0][0].db_name} & #{array[j].values[0][0].db_name}..."
        other = array[j].clone
        array[i].each_key do |key|
          puts "================ Matching in Group \"#{key}\" =================="
          movie_key_compared << key
          pairwise_match_movie_odm(array[i][key], other[key])
        end
        other.each_key do |key|
          unless movie_key_compared.include? key
            print "#{key}, "
            pairwise_match_movie_odm(nil, other[key])
          end
        end
      end
    end
  end

  def self.actor_linkage
    puts "Reading from TMDB ..."
    Mongoid.load!('../data_models/mongoid.yml', :tmdb)
    tmdb_actors_orignal = TmdbActor.all
    tmdb_actors = Array.new
    tmdb_actors_orignal.each do |actor|
      tmdb_actor = FusedActor.parse_tmdb_actor actor
      tmdb_actors<< tmdb_actor
    end

    puts "Reading from IMDB ..."
    imdb_query = ImdbQuery.new
    imdb_ids = imdb_query.query_actordb_all
    imdb_actors = FusedActor.read_imdb_by_ids imdb_ids

    puts "Reading from WIKI ..."
    Mongoid.load!('../data_models/mongoid.yml', :wiki)
    wiki_actors_orignal = WikiActor.all
    wiki_actors = Array.new
    wiki_actors_orignal.each do |actor|
      wiki_actor = FusedActor.parse_wiki_actor actor
      wiki_actors<< wiki_actor
    end

    puts "TMDB #: #{tmdb_actors.length}"
    puts "IMDB #: #{imdb_actors.length}"
    puts "WIKI #: #{wiki_actors.length}"

    puts "Sorting by name ..."
    tmdb_actors.sort! { |a, b| a.name <=> b.name }
    imdb_actors.sort! { |a, b| a.name <=> b.name }
    wiki_actors.sort! { |a, b| a.name <=> b.name }

    puts "Initialize before_fused data in database ..."
    Mongoid.load!('../data_models/mongoid.yml', :before_fused)
    FusedActor.delete_all
    tmdb_actors.each do |actor|
      actor.save
    end
    imdb_actors.each do |actor|
      actor.save
    end
    # deduplicate
    flag = Hash.new(false)
    wiki_actors.length.times do |i|
      next if flag[i]
      for j in (i+1)..(wiki_actors.length-1)
        if (wiki_actors[i].equals wiki_actors[j]) && !(wiki_actors[i].eql? wiki_actors[j])
          flag[j] = true
        end
        if !(wiki_actors[i].equals wiki_actors[j])
          break
        end
      end
      wiki_actors[i].save
    end
    flag = nil
    wiki_actors = FusedActor.where({db_name: 'wiki'})
=begin
    wiki_actors.each do |actor|
      actor.save
    end
=end

# test group by
    puts "Grouping by first character of firstname and lastname ..."
    tmdb_actor_groups = FusedActor.group_by_first_char tmdb_actors
    imdb_actor_groups = FusedActor.group_by_first_char imdb_actors
    wiki_actor_groups = FusedActor.group_by_first_char wiki_actors

    tmdb_count = 0
    imdb_count = 0
    wiki_count = 0

    tmdb_actor_groups.values.each do |array|
      tmdb_count += array.length
    end
    imdb_actor_groups.values.each do |array|
      imdb_count += array.length
    end
    wiki_actor_groups.values.each do |array|
      wiki_count += array.length
    end
    puts "tmdb #: #{tmdb_count}"
    p tmdb_actor_groups.keys
    puts "imdb #: #{imdb_count}"
    p imdb_actor_groups.keys
    puts "wiki #: #{wiki_count}"
    p wiki_actor_groups.keys

    puts tmdb_actor_groups.length
    puts imdb_actor_groups.length
    puts wiki_actor_groups.length

    pairwise_match_actor_groups([tmdb_actor_groups, imdb_actor_groups, wiki_actor_groups])

    puts "FusedActor.current_match_id = #{FusedActor.current_match_id}"
  end

  def self.movie_linkage
    puts "Reading from TMDB ..."
    Mongoid.load!('../data_models/mongoid.yml', :tmdb)
    tmdb_movies_orignal = TmdbMovie.all
    tmdb_movies = Array.new
    tmdb_movies_orignal.each do |movie|
      tmdb_movie = FusedMovie.parse_tmdb_movie movie
      tmdb_movies<< tmdb_movie
    end

    puts "Reading from IMDB ..."
    imdb_query = ImdbQuery.new
    imdb_ids = imdb_query.query_moviedb_all
    imdb_movies = FusedMovie.read_imdb_by_ids imdb_ids

    puts "Reading from WIKI ..."
    Mongoid.load!('../data_models/mongoid.yml', :wiki)
    wiki_movies_orignal = WikiFilm.all
    wiki_movies = Array.new
    wiki_movies_orignal.each do |movie|
      wiki_movie = FusedMovie.parse_wiki_movie movie
      wiki_movies<< wiki_movie
    end

    puts "TMDB #: #{tmdb_movies.length}"
    puts "IMDB #: #{imdb_movies.length}"
    puts "WIKI #: #{wiki_movies.length}"

    puts "Sorting ..."
    tmdb_movies.sort! { |a, b| a.title <=> b.title }
    imdb_movies.sort! { |a, b| a.title <=> b.title }
    wiki_movies.sort! { |a, b| a.title <=> b.title }
    puts wiki_movies[0].db_name

    puts "Initialize before_fused data in database ..."
    Mongoid.load!('../data_models/mongoid.yml', :before_fused)
    FusedMovie.delete_all
    tmdb_movies.each do |movie|
      movie.save
    end
    imdb_movies.each do |movie|
      movie.save
    end
    # deduplicate
    flag = Hash.new(false)
    wiki_movies.length.times do |i|
      next if flag[i]
      for j in (i+1)..(wiki_movies.length-1)
        if (wiki_movies[i].equals wiki_movies[j]) && !(wiki_movies[i].eql? wiki_movies[j])
          flag[j] = true
        end
        unless (wiki_movies[i].equals wiki_movies[j])
          break
        end
      end
      wiki_movies[i].save
    end
    flag = nil
    wiki_movies = FusedMovie.where({db_name: 'wiki'})

    # test group by
    puts "Grouping by first character of title ..."
    tmdb_movie_groups = FusedMovie.group_by_first_char tmdb_movies
    imdb_movie_groups = FusedMovie.group_by_first_char imdb_movies
    wiki_movie_groups = FusedMovie.group_by_first_char wiki_movies


    tmdb_count = 0
    imdb_count = 0
    wiki_count = 0

    tmdb_movie_groups.values.each do |array|
      tmdb_count += array.length
    end
    imdb_movie_groups.values.each do |array|
      imdb_count += array.length
    end
    wiki_movie_groups.values.each do |array|
      wiki_count += array.length
    end
    puts "tmdb #: #{tmdb_count}"
    puts "imdb #: #{imdb_count}"
    puts "wiki #: #{wiki_count}"

    puts tmdb_movie_groups.length
    puts imdb_movie_groups.length
    puts wiki_movie_groups.length

    pairwise_match_movie_groups([tmdb_movie_groups, imdb_movie_groups, wiki_movie_groups])

    puts "FusedMovie.current_match_id = #{FusedMovie.current_match_id}"
  end
end


