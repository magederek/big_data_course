require_relative './data_fusion'

# Run the data fusion methods
puts "Reading from Database ..."
data_fusion = DataFusion.new
puts "Read Done!"

=begin
puts data_fusion.linked_actor.length
puts data_fusion.linked_movie.length
=end



#data_fusion.fuse_movie
data_fusion.fuse_actor
