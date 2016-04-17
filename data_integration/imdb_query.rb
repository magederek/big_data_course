#Author @QI, XIAOXU
#Date   @2016-04-14

require 'json'
require 'rubygems'
require 'mongo'

class ImdbQuery
  attr_reader :address_port, :client

  def initialize address_port = '127.0.0.1:27018'
    @client = Mongo::Client.new([address_port], :database => 'movieactor', :monitoring => false)
    @address_port = address_port
  end

  def query_moviedb_by_year(year)
    array = Array.new
    documents = client[:movie].find(:Year => year)
    documents.each do |document|
      id = document["_id"].to_str
      array.push(id)
    end
    #puts documents.count
    return array
  end

  def get_movie_cluster_by_year()
    movie_cluster = Hash.new
    for year in 1962..2016
      #puts year
      array = query_moviedb_by_year(year)
      movie_cluster[year] = array
      #puts array
    end
    #puts movieCluster
    return movie_cluster
  end

  def query_actordb_by_year(year)
    year = year.to_s
    array = Array.new
    documents = client[:actor].find(:birthday => /#{year}/)
    documents.each do |document|
      id = document["_id"].to_str
      array.push(id)
      #puts document["Birthday"]
      #puts document["Name"]
    end
    #puts documents.count
    return array
  end

  def get_actor_cluster_by_year()
    actor_cluster = Hash.new
    for year in 1881..2010
      #puts year
      array = query_actordb_by_year(year)
      actor_cluster[year]=array
      #puts array
    end
    #puts movieCluster
    return actor_cluster
  end

  def query_actordb_all
    array = Array.new
    documents = client[:actor].find
    documents.each do |document|
      id = document["_id"].to_str
      array.push(id)
    end
    return array
  end
end


# puts get_actor_cluster_by_year
# puts get_movie_cluster_by_year
# ids = ImdbQuery.query_moviedb_by_year(2015)
# client = Mongo::Client.new([@@address_port], :database => 'movieactor', :monitoring => false)
# ids.each do |id|
#   doc = client[:movie].find(:_id => BSON::ObjectId(id)).first
#   puts doc["Title"]
  # puts doc["Year"]
  # puts doc["Directors"]
# end

