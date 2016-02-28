#!/usr/bin/ruby

require 'httparty'
require 'nokogiri'
require 'fileutils'

class TMDbCrawler
  include HTTParty
  attr_reader :SavePathBase
  @@SavePathBase = '/home/derek/TMDb_pages'
  @@LogFile = "#{@@SavePathBase}/log_#{Time.now.strftime('%Y%m%d-%H%M%S')}"
  puts @@LogFile
  @@cookie_base
  @@headers_field = { 
    "Accept" => "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Encoding" => "gzip, deflate, br",
    "Accept-Language" => "en-US,zh;q=0.7,en;q=0.3",
    "Connection" => "keep-alive",
    "DNT" => "1",
    "Host" => "www.themoviedb.org",
    "User-Agent" => "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:44.0) Gecko/20100101 Firefox/44.0",
  }
  # Queue for multi-analyzer methods to analyze consecutively
  @@analysis_queue_movie_list = Queue.new
  @@analysis_queue_movie_page = Queue.new 
  @@analysis_queue_crew = Queue.new 
  @@analysis_queue_cast = Queue.new
  @@analysis_queue_reviews = Queue.new

  @@mutex = Mutex.new

  # configurate the defatult params of HTTParty
  # base_uri 'http://www.baidu.com'
  base_uri 'https://www.themoviedb.org'
  headers @@headers_field
  default_timeout 8 # default_timeout = 5 seconds
  format :html

  # begin to craw all the information of movies
  def self.begin_crawl_movies
    analyze_moive_page_thread = Thread.new do
      idle_sec = 0
      while true 
        begin
        if analyze_movie_page
          idle_sec = 0
          next
        elsif idle_sec > 10
          puts "analyzed done!"
          break
        else
          sleep 1 
          idle_sec += 1
          puts "waiting for analyzing ..." if [4, 6, 8].include? idle_sec
          Thread.pass
        end
        rescue => ex
          puts ex
        end
      end
    end
    # analyze_moive_page_thread.run
    FileUtils.mkdir_p @@SavePathBase unless Dir.exist? @@SavePathBase
    url_path = '/discover/movie'
    movie_list_query = {
      "page" => "1",
      "language" => "en",
      "primary_release_year" => "0",
      "sort_by" => "title.asc",
      "vote_count.gte" => "0",
      "media_type" => "movie" 
    }

    # download the first page of movie list
    # to get Cookies and total_pages_number
    begin
    main_page = get url_path, query: movie_list_query
    rescue => ex
      puts ex.class
      puts "Exception occured: #{ex}"
      puts "Cannot connect to url: #{base_uri}#{url_path}"
      puts "Getting total page number failed! Exit!"
      exit 1
    end

    # set up the cookie base for the whole website
    @@cookie_base = main_page.headers['Set-Cookie'].split(';')[0]

    # convert HTTParty::Response to Nokogiri::HTML::Document
    main_page = Nokogiri::HTML(main_page)

    # get total page_number for iteration
    total_pages_number = get_pages_number main_page 
    puts "Total page of movies: #{total_pages_number}"


    for page_number in 1..2
    # for page_number in 1..total_pages_number.to_i
      movie_list_query["page"] = page_number.to_s

      # combine cookie base with GA's cookies
      cookie_movie_list = "; _ga=GA1.2.1734132844.#{Time.now.to_i}; _gat=1"
      cookie = @@cookie_base + cookie_movie_list
      @@headers_field['Cookie'] = cookie

      # reallocate the cookie in headers
      headers @@headers_field

      # download the movie_list_page, and get the file_path
      list_file_path = download_movie_list(url_path, movie_list_query)

      # get all the movie_urls from the list_page
      movie_urls = analyze_movie_list(list_file_path)

      # download all the movie_pages within the list_page
      download_movie_threads = []
      # create a copy of iterator to avoid reading incorrect number in threadds
      tmp_page_number = page_number
      # begin handling each movie_urls using multi-threads
      movie_urls.each do |movie_url|
        thread = Thread.new do
          sleep(Random.rand / 5)  # each thread sleeps for 0-200ms, to avoid concurrent running
          movie_file_path = download_movie_page(movie_url, tmp_page_number)
          # call analyze_movie_page to analyze the content using new threads
          @@analysis_queue_movie_page << movie_file_path if movie_file_path != nil
        end
        # download_movie_threads << thread
      end
      # download_movie_threads.each {|thread| thread.run(5)}
    end
    while Thread.list.size != 1
      sleep 1
    end
  end

  # get the total number of pages
  def self.get_pages_number(nokogiri_html)
    nokogiri_html = nokogiri_html.at_css "div.outer_wrap main#main div.media section.main_content div.results div.pagination p.left"
    nokogiri_html.content =~ /^.*?\d+.*?(\d+).*/
    return $1
  end
  # download the specific movie_list_page to @@SavePathBase/movie_list
  # named by the page number
  # RETURN: the relative path of the saved file
  def self.download_movie_list(url_path, query)
    begin
      timeout_try = 0
      movie_list_page = get url_path, query: query
    rescue Net::OpenTimeout
      timeout_try += 1
      retry unless timeout_try > 3
    rescue => ex
      save_to_log("Failed: download #{base_uri}#{url_path}: #{ex}")
    end
    file_path = "#{@@SavePathBase}/movie_list/#{query["page"]}.html"
    file_dir = "#{@@SavePathBase}/movie_list" 
    FileUtils.mkdir_p file_dir unless Dir.exist? file_dir
    File.open file_path, "w" do |file|
      file.write movie_list_page
    end
    return "movie_list/#{query["page"]}.html"
  end
  # get movies' urls in the specific page and download all the movie pages
  # RETURN: an Array of urls of the movies in this list page
  def self.analyze_movie_list(file_path)
    movie_urls = Array.new
    File.open("#{@@SavePathBase}/#{file_path}") do |file|
      movie_list_page = Nokogiri::HTML(file)
      movie_list_page.css("div.outer_wrap main#main div.media section.main_content div.results div[class='item poster card'] div.info").each do |info|
        movie_urls << info.at_css("p a[class='title result']")["href"]
      end
    end
    return movie_urls
  end
  # download the specific movie__page to @@SavePathBase/#{page_num}
  # named by the movie_id
  # RETURN: the relative path of the saved file
  def self.download_movie_page(url_path, page_num = 1, query = {})
    # get the ID and INTRO_LANG of the movie
    movie_id = url_path.split('/')[-1].split('?')[0].split('-')[0]
    movie_intro_lang = url_path.split('=')[-1]
    file_path = "#{@@SavePathBase}/movies/#{page_num}/#{movie_id}_#{movie_intro_lang}.html"
    file_dir = "#{@@SavePathBase}/movies/#{page_num}"
    FileUtils.mkdir_p file_dir unless Dir.exist? file_dir
    # starting download the movie page
    puts "downloading #{base_uri}/#{url_path}"
    $stdout.flush
    begin
      movie_page = get url_path
    rescue Net::OpenTimeout
      timeout_try += 1
      retry unless timeout_try > 2
    rescue => ex
      save_to_log("Failed: download #{base_uri}#{url_path} page=#{page_num}: #{ex}")
      return nil
    end
    # create a directory named by ID for each movie
    File.open file_path, "w" do |file|
      file.write movie_page
    end
    return file_path
  end

  # analyze the movie's page to get brief information
  def self.analyze_movie_page
    if @@analysis_queue_movie_page.empty?
      return false
    end
    movie_file_path = @@analysis_queue_movie_page.shift
    # puts "analyzing #{movie_file_path} ..."
    # $stdout.flush

    movie_page = nil
    File.open movie_file_path, "r" do |file|
      movie_page = Nokogiri::HTML(file)
    end
    movie = Hash.new
    title_html = movie_page.at_css 'html body div#container div#movie div#mainCol div.title h2#title a span' 
    if title_html == nil
      other_lang_url_html = movie_page.at_css 'html body div#container div#movie.new div#mainCol.new div.carton div.content ul li a' 
      if other_lang_url_html != nil
        puts other_lang_url_html['href']
        Thread.new do
          other_lang_file_path = download_movie_page other_lang_url_html['href'], 'non-en'
          @@analysis_queue_movie_page << other_lang_file_path if other_lang_file_path != nil
        end
      end
      return true
    end
    movie['title'] = title_html.content
    # movie['language'] = 
    puts movie.inspect

    # download crew and cast page
    crew_url_html = movie_page.at_css 'html body div#container div#movie div#mainCol p.more a'
    crew_url_html = nil
    if crew_url_html != nil && crew_url_html['href'] != nil
      crew_url = crew_url_html['href']
      crew_page = nil
      begin
        crew_page = get crew_url
      rescue Net::OpenTimeout
        timeout_try += 1
        retry unless timeout_try > 2
      rescue => ex
        save_to_log("Failed: download #{base_uri}#{url_path}: #{ex}")
      end
      crew_page = Nokogiri::HTML(crew_page)
        table_name = table['id']
        directing = Hash.new
        production = Hash.new
        cast = Hash.new
        directing_table = crew_page.at_css 'html body div#container div#movie div#mainCol table#Directing tbody'
        directing_table.css 'tr' do |row|
          director_name = row.css('td a') == nil ? row.css('td')[0].content : directing_table.css('td a').content
          director_position = row.css('td')[1].content
        end
      end

    # call analyze_crew_list to get information about crews

    end

    # download cast-list page named by ID_cast.html
    begin
    rescue Net::OpenTimeout
      timeout_try += 1
      retry unless timeout_try > 2
    rescue => ex
      save_to_log("Failed: download #{base_uri}#{url_path}: #{ex}")
    end
    
    # call analyze_cast_list to get information about cast


    # download reviews page named by ID_reviews.html
    begin
    rescue Net::OpenTimeout
      timeout_try += 1
      retry unless timeout_try > 2
    rescue => ex
      save_to_log("Failed: download #{base_uri}#{url_path}: #{ex}")
    end
    
    # call analyze_reviews to get information about reviews
    

    # download recommandation page named by ID_crew.html
    begin
    rescue Net::OpenTimeout
      timeout_try += 1
      retry unless timeout_try > 2
    rescue => ex
      save_to_log("Failed: download #{base_uri}#{url_path}: #{ex}")
    end
    
    # call analyze_recommandation to get information about recommandation
    
    return true
  end

  # analyze the crew list to get all crew brief information
  def self.analyze_crew_list(crew_file_path)
  end

  # analyze the crew list to get all crew brief information
  def self.analyze_cast_list(cast_file_path)
  
  end

  # analyze the reviews of the movie for further analysis
  def self.analyze_reviews(reviews_file_path)

  end

  # analyze the recommandation page for further analysis
  def self.analyze_recommandation(recommandation_file_path)

  end

  def self.save_to_log(message)
    @@mutex.lock
    File.open @@LogFile, "a" do |file|
      file.write(message + "\n")
    end
    @@mutex.unlock
  end

end

TMDbCrawler.begin_crawl_movies



