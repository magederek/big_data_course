require 'jaro_winkler'
require 'jaccard'
require_relative './jaccard_n_grams.rb'
class MongeElkan
  def self.jaro_winkler_sim(str1, str2)
    str1.gsub!(/,|_|-/, ' ')
    str2.gsub!(/,|_|-/, ' ')
    x = str1.split(/\s+/)
    y = str2.split(/\s+/)
    sum_sim = 0
    x.length.times do |i|
      max_sim = 0
      y.length.times do |j|
        if JaroWinkler.distance(x[i], y[j], ignore_case: true) > max_sim
          max_sim = JaroWinkler.distance(x[i], y[j], ignore_case: true)
        end
      end
      sum_sim += max_sim
    end
    return sum_sim / x.length
  end

  def self.jaccard_bigrams_sim(str1, str2)
    str1.gsub!(/,|_|-/, ' ')
    str2.gsub!(/,|_|-/, ' ')
    x = str1.split(/\s+/)
    y = str2.split(/\s+/)
    sum_sim = 0
    x.length.times do |i|
      max_sim = 0
      y.length.times do |j|
        if JaccardNGrams.bigrams_sim(x[i], y[j]) > max_sim
          max_sim = JaccardNGrams.bigrams_sim(x[i], y[j])
        end
      end
      sum_sim += max_sim
    end
    return sum_sim / x.length
  end
  
  def self.jaro_winkler_simavg(str1, str2)
    return (jaro_winkler_sim(str1, str2) + jaro_winkler_sim(str2, str1)) / 2
  end

  def self.jaccard_bigrams_simavg(str1, str2)
    return (jaccard_bigrams_sim(str1, str2) + jaccard_bigrams_sim(str2, str1)) / 2
  end
end   


a = [" J", "Jo", "on", "ne", "es", "s "]
b = [" J", "Jo", "oh", "hn", "ns", "so", "on", "n "]
#puts Jaccard.coefficient(a, b)
puts MongeElkan.jaro_winkler_sim("George Bush", "George W. Bush");
puts MongeElkan.jaro_winkler_sim("George W. Bush", "George Bush");
puts MongeElkan.jaro_winkler_simavg("George W. Bush", "George Bush");
puts MongeElkan.jaro_winkler_simavg("B. Obama", "Barack Obama")
puts MongeElkan.jaro_winkler_simavg("Paul Jones", "Jones, Paul")
puts MongeElkan.jaro_winkler_simavg("Paul", "Pual")

