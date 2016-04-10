require_relative "./monge_elkan.rb"
require_relative "./jaccard_n_grams.rb"

puts "====="
puts MongeElkan.jaro_winkler_simavg("Derek Robert Joseph Beckham", "David R. J. Beckham")
puts JaccardNGrams.bigrams_sim("Derek Robert Joseph Beckham", "David R. J. Beckham")
puts MongeElkan.jaccard_bigrams_simavg("Derek Robert Joseph Beckham", "David R. J. Beckham")
puts "---------------------------------"
puts "Jack Jhon vs. Jack John"
puts "Bigrams: #{JaccardNGrams.bigrams_sim("Jack Jhon", "Jack John")}"
puts "Trigrams: #{JaccardNGrams.trigrams_sim("Jack Jhon", "Jack John")}"
puts "MongeElkan + JaroWinkler: #{MongeElkan.jaro_winkler_simavg("Jack Jhon", "Jack John")}"
puts "MongeElkan + Jaccard(Bigrams): #{MongeElkan.jaccard_bigrams_simavg("Jack Jhon", "Jack John")}"
puts "===================="
s1 = "Dixon"
s2 = "Dicksonx"
puts "Bigrams: #{JaccardNGrams.bigrams_sim(s1, s2)}"
puts "Trigrams: #{JaccardNGrams.trigrams_sim(s1, s2)}"
puts "MongeElkan + JaroWinkler: #{MongeElkan.jaro_winkler_simavg(s1, s2)}"
puts "Jaro: #{JaroWinkler.jaro_distance(s1, s2)}"
puts "MongeElkan + Jaccard(Bigrams): #{MongeElkan.jaccard_bigrams_simavg(s1, s2)}"
puts "===================="
