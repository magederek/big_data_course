require "jaccard"
class JaccardNGrams
  def self.bigrams_sim(str1, str2)
    str1 = ' ' + str1 + ' '
    str2 = ' ' + str2 + ' '
    str1.downcase!
    str2.downcase!
    x = []
    y = []
    (str1.length - 1).times do |i|
      x << str1.slice(i, 2)
    end
    (str2.length - 1).times do |i|
      y << str2.slice(i, 2)
    end
    return Jaccard.coefficient(x, y)
  end

  def self.trigrams_sim(str1, str2)
    str1 = '  ' + str1 + '  '
    str2 = '  ' + str2 + '  '
    str1.downcase!
    str2.downcase!
    x = []
    y = []
    (str1.length - 2).times do |i|
      x << str1.slice(i, 3)
    end
    (str2.length - 2).times do |i|
      y << str2.slice(i, 3)
    end
    return Jaccard.coefficient(x, y)
  end
end

# puts JaccardNGrams.bigrams_sim("Paul", "Pual")
# puts JaccardNGrams.trigrams_sim("Paul", "Pual")
# puts JaccardNGrams.trigrams_sim("Johnson", "Jones")
# puts JaccardNGrams.bigrams_sim("B. Obama", "Barack Obama")
# puts JaccardNGrams.trigrams_sim("B. Obama", "Barack Obama")
