require_relative './fused_actor'
require_relative './tmdb_actor'
require_relative './imdb_query'

class EntityResolution
  def self.pairwise_match_actor_ids actors_hash1, actors_hash2
    return nil if actors_hash1.class != Hash || actors_hash2.class != Hash
    actors1 = actors_hash1.values[0]
    actors2 = actors_hash2.values[0]
    db1 = actors_hash1.keys[0]
    db2 = actors_hash2.keys[0]
    matches = Array.new  # 2-D Array: [{db1 => actor1._id, db2 => actor2._id, ...}, ...]
    #(actors1.length).times do |i|
    100.times do |i|
      match = Hash.new
      (actors1.length).times do |j|
        if actors1[i].match? actors2[j]
          puts "#{i} match #{j}"
          match.merge!({db2 => actors2[j]._id.to_s})
          actors2.slice!(j)
          break
        end
      end
      match.merge!({db1 => actors1[i]._id.to_s})
      matches << match
      puts "Comparing #{i} ..." if i % 10 == 0
    end
    matches
  end

  def self.pairwise_match_actor_odm actors_hash1, actors_hash2
    return nil if actors_hash1.class != Hash || actors_hash2.class != Hash
    actors1 = actors_hash1.values[0]
    actors2 = actors_hash2.values[0]
    db1 = actors_hash1.keys[0]
    db2 = actors_hash2.keys[0]
    matches = Array.new  # 2-D Array: {index => [actor1._id, actor2._id, ...]}
    (actors1.length).times do |i|
    #100.times do |i|
      match = Hash.new
      (actors1.length).times do |j|
        if actors1[i].match? actors2[j]
          puts "#{i} match #{j}"
          match.merge!({db2 => actors2[j]})
          actors2.slice!(j)
          break
        end
      end
      match.merge!({db1 => actors1[i]})
      matches << match
      puts "Comparing #{i} ..." if i % 10 == 0
    end
    matches
  end
end

tmdb_actors_orignal = TmdbActor.all
tmdb_actors = Array.new
tmdb_actors_orignal.each do |actor|
  tmdb_actor = FusedActor.parse_tmdb_actor actor
  tmdb_actors << tmdb_actor
end

imdb_query = ImdbQuery.new
imdb_ids = imdb_query.query_actordb_all
imdb_actors = FusedActor.read_imdb_by_ids imdb_ids

puts tmdb_actors.length
puts imdb_actors.length

matches_tmdb_imdb = EntityResolution.pairwise_match_actor_odm({tmdb: tmdb_actors}, {imdb: imdb_actors})
puts matches_tmdb_imdb.length
matches_tmdb_imdb.each do |match|
  if match.length > 1
    match.each_value do |value|
      puts "#{value.name}: #{value.birthday}"
      #puts value.birthday
    end
    puts
  end
end

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