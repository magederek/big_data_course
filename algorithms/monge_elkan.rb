require 'jaro_winkler'
require 'jaccard'
require_relative './jaccard_n_grams.rb'
class MongeElkan
  def self.jaro_winkler_sim(str1, str2)
    str1.gsub!(/,|_|-/, ' ')
    str2.gsub!(/,|_|-/, ' ')
    x = str1.split(/\s+/)
    y = str2.split(/\s+/)
    if x.length == 0 || y.length == 0
      return 0.0
    end
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

  def self.name_array_sim(array1, array2)
    sum_sim = 0
    array1.length.times do |i|
      max_sim = 0
      array2.length.times do |j|
        if jaro_winkler_simavg(array1[i], array2[j]) > max_sim
          max_sim = jaro_winkler_simavg(array1[i], array2[j])
        end
      end
      sum_sim += max_sim
    end
    return sum_sim / array1.length
  end

  def self.exact(str1, str2)
    str1.gsub!(/,|_|-/, ' ')
    str2.gsub!(/,|_|-/, ' ')
    x = str1.split(/\s+/)
    y = str2.split(/\s+/)
    sum_sim = 0
    x.length.times do |i|
      max_sim = 0
      y.length.times do |j|
        if x[i] == y[j]
          exact_sim = 1.0
        else
          exact_sim = 0.0
        end
        if exact_sim > max_sim
          max_sim = exact_sim
        end
      end
      sum_sim += max_sim
    end
    return sum_sim / x.length
  end
end   


# a = [" J", "Jo", "on", "ne", "es", "s "]
# b = [" J", "Jo", "oh", "hn", "ns", "so", "on", "n "]
# puts Jaccard.coefficient(a, b)
# puts MongeElkan.jaro_winkler_sim("George Bush", "George W. Bush");
# puts MongeElkan.jaro_winkler_sim("George W. Bush", "George Bush");
# puts MongeElkan.jaro_winkler_simavg("George W. Bush", "George Bush");
# puts MongeElkan.jaro_winkler_simavg("B. Obama", "Barack Obama")
# puts MongeElkan.jaro_winkler_simavg("Paul Jones", "Jones, Paul")
# puts MongeElkan.jaro_winkler_simavg("Paul", "Pual")

