require_relative './fused_actor'
require_relative './fused_movie'
require 'pp'

class EntityResolution
  def self.pairwise_match_actor_odm actors1, actors2
    Mongoid.load!("./mongoid.yml", :before_fused)
    #return nil if actor_hash1.class != Hash || actor_hash2.class != Hash
    #actors1 = actor_hash1.values[0]
    #actors2 = actor_hash2.values[0]
    if (actors1.nil? || actors1.length == 0) && (actors2.nil? || actors2.length == 0)
      return []
    elsif actors1.nil? || actors1.length == 0
      matches = []
      actors2.each do |actor|
        #if actor.match_id.nil?
        #  actor.match_id = FusedActor.current_match_id
        #  FusedActor.current_match_id += 1
        #end
        matches << [actor]
      end
      return matches
    elsif actors2.nil? || actors2.length == 0
      matches = []
      actors1.each do |actor|
        #if actor.match_id.nil?
        #  actor.match_id = FusedActor.current_match_id
        #  FusedActor.current_match_id += 1
        #end
        matches << [actor]
      end
      return matches
    else
      matches = Array.new
      (actors1.length).times do |i|
        #100.times do |i|
        match = Array.new
        match_flag = false
        (actors2.length).times do |j|
          if actors1[i].match? actors2[j]
            match_flag = true
            if !actors1[i].match_id.nil?
              actors2[j].match_id = actors1[i].match_id
            elsif !actors2[j].match_id.nil?
              actors1[i].match_id = actors2[j].match_id
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
          #if actors1[i].match_id.nil?
          #  actors1[i].match_id = FusedActor.current_match_id
          #  FusedActor.current_match_id += 1
          #end
          matches << [actors1[i]]
        end
        puts "Comparing actor #{i} ..." if i % 10 == 0
      end
      actors2.length.times do |i|
        #if actors2[i].match_id.nil?
        #  actors2[i].match_id = FusedActor.current_match_id
        #  FusedActor.current_match_id += 1
        #end
        matches << [actors2[i]]
      end
      return matches
    end
  end

  def self.pairwise_match_actor_groups array
    matches_tmdb_imdb = []
    for i in 0..(array.length - 1)
      for j in (i + 1)..(array.length - 1)
        matches_tmdb_imdb = []
        actor_key_compared = []
        puts "Begin to pairwise match #{array[i].values[0][0].db_name} & #{array[j].values[0][0].db_name}..."
        other = array[j].clone
        array[i].each_key do |key|
          puts "================ Matching in Group \"#{key}\" =================="
          actor_key_compared << key
          matches_tmdb_imdb.concat(pairwise_match_actor_odm(array[i][key], other[key]))
        end

        other.each_key do |key|
          unless actor_key_compared.include? key
            print "#{key}, "
            matches_tmdb_imdb.concat(pairwise_match_actor_odm(nil, other[key]))
          end
        end
      end
    end
    matches_tmdb_imdb
  end

  # input : ({db1 => movies2}, {db2 => movies2})
  def self.pairwise_match_movie_odm movie_hash1, movie_hash2
    #return nil if movie_hash1.class != Hash || movie_hash2.class != Hash
    movies1 = movie_hash1.values[0]
    movies2 = movie_hash2.values[0]
    db1 = movie_hash1.keys[0]
    db2 = movie_hash2.keys[0]
    if movie_hash1.nil? && movie_hash2.nil?
      return nil
    elsif movie_hash1.values[0].nil?
      matches = []
      movie_hash2.values[0].each do |movie|
        matches << {db2 => movie}
      end
      return matches
    elsif movie_hash2.values[0].nil?
      matches = []
      movie_hash1.values[0].each do |movie|
        matches << {db1 => movie}
      end
      return matches
    else
      matches = Array.new  # Array: {index => {db =>movie1, db => movie2, ...}
      (movies1.length).times do |i|
        #100.times do |i|
        match = Hash.new
        (movies2.length).times do |j|
          if movies1[i].match? movies2[j]
            puts "#{i} match #{j}"
            match.merge!({db2 => movies2[j]})
            movies2.slice!(j)
            break
          end
        end
        match.merge!({db1 => movies1[i]})
        matches << match
        puts "Comparing Movie #{i} ..." if i % 10 == 0
      end
      movies2.length.times do |i|
        matches << {db2 => movies2[i]}
      end
      return matches
    end
  end

  def self.actor_linkage
    puts "Reading from TMDB ..."
    Mongoid.load!("./mongoid.yml", :tmdb)
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
    Mongoid.load!("./mongoid.yml", :wiki)
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
    Mongoid.load!("./mongoid.yml", :before_fused)
    FusedActor.delete_all
    tmdb_actors.each do |actor|
      actor.save
    end
    imdb_actors.each do |actor|
      actor.save
    end
    wiki_actors.each do |actor|
      actor.save
    end

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
    tmdb_actor_groups_clone = tmdb_actor_groups.clone
    imdb_actor_groups.values.each do |array|
      imdb_count += array.length
    end
    imdb_actor_groups_clone = imdb_actor_groups.clone
    wiki_actor_groups.values.each do |array|
      wiki_count += array.length
    end
    wiki_actor_groups_clone = imdb_actor_groups.clone
    puts "tmdb #: #{tmdb_count}"
    p tmdb_actor_groups.keys
    puts "imdb #: #{imdb_count}"
    p imdb_actor_groups.keys
    puts "wiki #: #{wiki_count}"
    p wiki_actor_groups.keys

    puts tmdb_actor_groups.length
    puts imdb_actor_groups.length
    puts wiki_actor_groups.length

    matches_tmdb_imdb = pairwise_match_actor_groups([tmdb_actor_groups, imdb_actor_groups, wiki_actor_groups])

    #puts "Begin to pairwise match IMDB & TMDB..."
    #actor_key_compared = []
    #matches_tmdb_imdb = []
    #tmdb_actor_groups.each_key do |key|
    #  puts "================ Matching in Group \"#{key}\" =================="
    #  actor_key_compared << key
    #  matches_tmdb_imdb.concat(pairwise_match_actor_odm({tmdb: tmdb_actor_groups[key]}, {imdb: imdb_actor_groups[key]}))
    #end

    #imdb_actor_groups.each_key do |key|
    #  unless actor_key_compared.include? key
    #    print "#{key}, "
    #    matches_tmdb_imdb.concat(pairwise_match_actor_odm({tmdb: nil}, {imdb: imdb_actor_groups[key]}))
    #  end
    #end


#matches_tmdb_imdb = EntityResolution.pairwise_match_actor_odm({tmdb: tmdb_actors}, {imdb: imdb_actors})
    puts "Entities found: #{matches_tmdb_imdb.length}"
    pairwise_matches = 0
    matches_tmdb_imdb.each do |match|
      if match.length > 0
        pairwise_matches += 1
        match.each do |value|
          #puts "#{value.name}: #{value.birthday}, #{value.place_of_birth}, #{value.known_for}"
          puts "#{value.name}: #{value.match_id}"
        end
        puts
      end
    end
    puts "FusedActor.current_match_id = #{FusedActor.current_match_id}"
    puts "Pairwise Matches = #{pairwise_matches}"
  end

  def self.movie_linkage
    puts "Reading from TMDB ..."
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
#imdb_actors = imdb_actors[0, 500]

    puts "TMDB #: #{tmdb_movies.length}"
    puts "IMDB #: #{imdb_movies.length}"

    puts "Sorting ..."
    tmdb_movies.sort! { |a, b| a.title <=> b.title }
    imdb_movies.sort! { |a, b| a.title <=> b.title }

    # test group by
    puts "Grouping by first character of title ..."
    tmdb_movie_groups = FusedMovie.group_by_first_char tmdb_movies
    imdb_movie_groups = FusedMovie.group_by_first_char imdb_movies

    tmdb_count = 0
    imdb_count = 0

    tmdb_movie_groups.values.each do |array|
      tmdb_count += array.length
    end
    imdb_movie_groups.values.each do |array|
      imdb_count += array.length
    end
    puts "tmdb #: #{tmdb_count}"
    puts "imdb #: #{imdb_count}"

    puts tmdb_movie_groups.length
    puts imdb_movie_groups.length

    puts "Begin to pairwise match ..."
    movie_key_compared = []
    matches_tmdb_imdb = []
    tmdb_movie_groups.each_key do |key|
      puts "================ Matching in Group \"#{key}\" =================="
      movie_key_compared << key
      matches_tmdb_imdb.concat(pairwise_match_movie_odm({tmdb: tmdb_movie_groups[key]}, {imdb: imdb_movie_groups[key]}))
    end

    p movie_key_compared

    imdb_movie_groups.each_key do |key|
      unless movie_key_compared.include? key
        print "#{key}, "
        imdb_movie_groups[key].each do |movie|
          matches_tmdb_imdb << {imdb: movie}
        end
      end
    end

    #matches_tmdb_imdb = EntityResolution.pairwise_match_movie_odm({tmdb: tmdb_movies}, {imdb: imdb_movies})
    puts "Entities found: #{matches_tmdb_imdb.length}"
    pairwise_matches = 0
    matches_tmdb_imdb.each do |match|
      if match.length > 1
        pairwise_matches += 1
        match.each_value do |value|
          puts "#{value.title}: #{value.year}, #{value.directors}, #{value.main_casts}"
          #puts value.birthday
        end
        puts
      end
    end
    puts "Pairwise Matches = #{pairwise_matches}"

  end
end

EntityResolution.actor_linkage



#(actors_array_1.length).times do |i|
#  sim = []
#  (actors_array_1.length).times do |j|
#    unless i == j
#      if actors_array_1[i].match? actors_array_1[j]
##        puts "#{i} match #{j}"
#        break
#      end
#      #sim << (actors_array_1[i].similarity actors_array_1[j])
#    end
#  end
#  puts "#{i} false"
  #sim.sort! { |x, y| y <=> x}
  #puts sim.max
  #p sim[0]
# end