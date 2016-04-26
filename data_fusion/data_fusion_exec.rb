require_relative './data_fusion'

# Run the data fusion methods
puts "Reading from Database ..."
data_fusion = DataFusion.new
puts "Read Done!"
puts data_fusion.linked_actor.length
puts data_fusion.linked_movie.length
=begin
data_fusion.linked_actor.each do |match_actors|
  if match_actors.length > 2 && (match_actors[0].name != match_actors[1].name || match_actors[0].name != match_actors[2].name || match_actors[1].name != match_actors[2].name)
    match_actors.each do |actor|
      puts "#{actor.match_id}: #{actor.name}"
    end
    puts
  end
end

data_fusion.linked_movie.each do |match_movies|
  if match_movies.length > 2 && (match_movies[0].title != match_movies[1].title || match_movies[0].title != match_movies[2].title || match_movies[1].title != match_movies[2].title)
    match_movies.each do |movie|
      puts "#{movie.match_id}: #{movie.title}"
    end
    puts
  end
end
=end

data_fusion.fuse_movie
data_fusion.fuse_actor