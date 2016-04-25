require_relative './fused_actor'
require_relative './fused_movie'
require 'pp'
require 'json'

class DataFusion
  # @linked_actor: 2-D Array[][], the actors with the same match_id
  #                will be grouped in the same first dimension.
  # @linked_movie: 2-D Array[][], the movies with the same match_id
  #                will be grouped in the same first dimension.
  attr_accessor :linked_actor, :linked_movie

  def initialize
    @linked_actor = []
    @linked_movie = []
    Mongoid.load!("./mongoid.yml", :before_fused)
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
    Mongoid.load!("./mongoid.yml", :fused)
  end

  def fuse_movie
    Mongoid.load!("./mongoid.yml", :fused)
  end
end

puts "Reading from Database ..."
data_fusion = DataFusion.new
puts "Read Done!"
puts data_fusion.linked_actor.length
data_fusion.linked_actor.each do |match_actors|
  if match_actors.length > 1
    match_actors.each do |actor|
      puts "#{actor.match_id}: #{actor.name}"
    end
    puts
  end
end
#puts data_fusion.linked_actor.length
#puts data_fusion.linked_movie.length