require_relative '../data_models/fused_actor'
require_relative '../data_models/fused_movie'
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
    Mongoid.load!('../data_models/mongoid.yml', :before_fused)
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
    Mongoid.load!('../data_models/mongoid.yml', :fused)
  end

  def fuse_movie
    Mongoid.load!('../data_models/mongoid.yml', :fused)
  end
end

