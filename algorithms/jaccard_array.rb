require_relative './monge_elkan.rb'

class JaccardArray
  def self.sim(array1, array2, threshold: 0.8)
    intersect = 0
    array1.length.times do |i|
      max_sim = 0
      array2.length.times do |j|
        if MongeElkan.jaro_winkler_simavg(array1[i], array2[j]) > max_sim
          max_sim = MongeElkan.jaro_winkler_simavg(array1[i], array2[j])
        end
      end
      # puts max_sim
      if max_sim > threshold
        intersect += 1
      end
    end
    return intersect.to_f / (array1.length + array2.length - intersect)
  end
end

puts JaccardArray.sim(["Barack Obama", "John", "Derek"], ["John", "B. Obama", "David"])
