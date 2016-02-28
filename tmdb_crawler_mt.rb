#!/usr/bin/ruby

require 'httparty'
require 'nokogiri'
require 'fileutils'
require 'json'

class TMDbCrawler
  include HTTParty
  attr_reader :SavePathBase
  # @@SavePathBase = 'E:/TMDb_pages'
  @@SavePathBase = '/home/derek/TMDb_pages'
  @@LogFile = "#{@@SavePathBase}/log_#{Time.now.strftime('%Y%m%d-%H%M%S')}.txt"
  @@MOVIES_DB_FILE = "#{@@SavePathBase}/db_#{Time.now.strftime('%Y%m%d-%H%M%S')}.json"
  @@analyze_movie_page_thread = nil
  @@TIMEOUT = 10 # default_timeout = 8 seconds
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
  # Variable for multi-analyzer methods to analyze consecutively
  @@analysis_queue_movie_list = Queue.new
  @@analyzing_list_threads = 0
  @@MAX_LIST_THREADS = 20 # 20 the better amoung my testing

  # Queue and Variable for analyzing movie pages
  # Simotineously maximun MAX_ANALYZING_THREADS pages
  @@analysis_queue_movie_page = Queue.new 
  @@analyzing_movie_threads = 0
  @@MAX_ANALYZING_THREADS = 40 * @@MAX_LIST_THREADS

  @@TOTAL_MOVIES = 0

  @@mutex = Mutex.new

  # configurate the defatult params of HTTParty
  # base_uri 'http://www.baidu.com'
  base_uri 'https://www.themoviedb.org'
  headers @@headers_field
  default_timeout @@TIMEOUT
  format :html

  # begin to craw all the information of movies
  def self.begin_crawl_movies
    GC.enable # make sure Ruby's Garbage Collection is enable
    FileUtils.mkdir_p @@SavePathBase unless Dir.exist? @@SavePathBase
    url_path = '/discover/movie'
    movie_list_query = {
      "page" => "1",
      "language" => "en",
      "primary_release_year" => "0",
      # "sort_by" => "title.asc",
      "sort_by" => "popularity.desc",
      "vote_count.gte" => "0",
      "media_type" => "movie" 
    }

    # download the first page of movie list
    # to get Cookies and total_pages_number
    retry_count = 0
    begin
      raise if retry_count > 2
      main_page = get url_path, query: movie_list_query
    rescue Net::OpenTimeout, Net::ReadTimeout
      puts "TIMEOUT, RETRYING ..."
      retry_count += 1
      retry
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

    # create the DB file of the current crawling
    begin
      File.open(@@MOVIES_DB_FILE, "w") do |file|
        file.puts '['
      end
    rescue
      puts "Fatal ERROR: cannot write file #{@@MOVIES_DB_FILE}"
      save_to_log("Fatal ERROR: cannot write file #{@@MOVIES_DB_FILE}")
    end

    # Create a thread to output the current count of movie analyzing threads
    Thread.new do
      while(true)
        sleep(1)
        puts "THREAD_SIZE: #{@@analyzing_movie_threads}"
        $stdout.flush
      end
    end

    # Iterately analyze every pages in every list
    for page_number in 1..30
      # create a copy of iterator to avoid reading incorrect number in threadds
      tmp_page_number = page_number.to_s
      movie_list_query_clone = movie_list_query.clone

      # Wait until current list analyzing threads is not full
      while @@analyzing_list_threads >= @@MAX_LIST_THREADS
        sleep(Random.rand / 2)
        Thread.pass
      end

      # Start a thread for downloading and analyzing a list page
      download_list_thread = Thread.new do
        @@mutex.lock
        @@analyzing_list_threads += 1
        @@mutex.unlock

        # for page_number in 1..total_pages_number.to_i
        movie_list_query_clone["page"] = tmp_page_number
        puts "Current Page: #{movie_list_query_clone["page"]}"
        $stdout.flush

        # combine cookie base with GA's cookies
        cookie_movie_list = "; _ga=GA1.2.1734132844.#{Time.now.to_i}; _gat=1"
        cookie = @@cookie_base + cookie_movie_list
        @@headers_field['Cookie'] = cookie

        # reallocate the cookie in headers
        headers @@headers_field

        # download the movie_list_page, and get the file_path
        list_file_path = download_movie_list(url_path, movie_list_query_clone)

        # get all the movie_urls from the list_page
        movie_urls = analyze_movie_list(list_file_path)

        # begin handling each movie_urls using multi-threads
        movie_urls.each do |movie_url|
          thread = Thread.new do
            sleep(Random.rand / 5)  # each thread sleeps for 0-200ms, to avoid concurrent running
            movie_file_path = download_movie_page(movie_url, tmp_page_number)

            # start the analyzing thread
            run_analyze_movie_thread

            # if download successfully, add it to the analysis queue pending,
            # which is arranged by run_analyze_movie_thread
            @@analysis_queue_movie_page << movie_file_path if movie_file_path != nil
          end
        end
        # analysis completed, quit the analyzing_list_threads
        @@mutex.lock
        @@analyzing_list_threads -= 1
        @@mutex.unlock
      end

      Thread.pass
      sleep 0.3 # MUST sleep for more than 0.2 secs to avoid the server block the connection 
      GC.start # start garbage collection
    end

    # Consider whether the crawl is done. When it is done,
    # only the main thread and the output status thread will be running
    while Thread.list.size != 2
      puts "Total Theards: #{Thread.list.size}"
      sleep 1
    end

    # Finally formating the DB file
    puts "Total Movies crawled: #{@@TOTAL_MOVIES}"
    system "sed -i '$s/,$//' #{@@MOVIES_DB_FILE}"
    system "sed -i '$a\]' #{@@MOVIES_DB_FILE}"
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
    #puts "HERE #{query["page"]}"
    timeout_try = 0
    begin
      movie_list_page = get url_path, query: query
    rescue
      timeout_try += 1
      sleep(Random.rand / 10)
      puts "retrying #{query['page']}"
      retry unless timeout_try > 5
      save_to_log("Fail[Download List Page #{query['page']} #{timeout_try} times] - #{ex}")
    end
    file_path = "#{@@SavePathBase}/movie_list/#{query['page']}.html"
    file_dir = "#{@@SavePathBase}/movie_list" 
    FileUtils.mkdir_p file_dir unless Dir.exist? file_dir
    begin
      # puts "HERE #{query['page']}"
      File.open file_path, "w" do |file|
        file.flock(File::LOCK_EX)
        file.write movie_list_page
        file.flock(File::LOCK_UN)
      end
    rescue
      sleep(Random.rand / 10)
      retry
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
    # puts "downloading #{base_uri}/#{url_path}"
    # $stdout.flush
    timeout_try = 0
    begin
      movie_page = get url_path
    rescue #Net::OpenTimeout, Net::ReadTimeout
      timeout_try += 1
      retry unless timeout_try > 5
      save_to_log("Fail[Download Movie Page in List #{page_num}]: - #{base_uri}#{url_path}: #{ex}")
      return nil
    end
    # create a directory named by ID for each movie
    File.open file_path, "w" do |file|
      file.write movie_page
    end
    return file_path
  end

  def self.run_analyze_movie_thread
    return if @@analyze_movie_page_thread != nil && @@analyze_movie_page_thread.status != false
    exit_timeout = 5 * @@TIMEOUT + 1
    # exit_timeout = 5
    warning_array = [exit_timeout*4/7, exit_timeout*5/7, exit_timeout*6/7]
    # puts exit_timeout
    # p warning_array
    puts("======== Run Thread Start =========")
    @@analyze_movie_page_thread = Thread.new do
      idle_sec = 0
      while true 
        begin
          if analyze_movie_page
            idle_sec = 0
            next
          elsif idle_sec > exit_timeout 
            # puts "analyzed done!"
            begin
              Thread.list.each do |thr|
                thr.run
                #puts thr.status
              end
            rescue
            end
            break
          else
            sleep 1 
            idle_sec += 1
            puts "waiting for analyzing ..." if warning_array.include? idle_sec
            Thread.pass
          end
        rescue => ex
          save_to_log("Fatal Fail: #{ex}")
        end
      end
      puts("======== Run Thread Exit =========")
    end
  end

  # analyze the movie's page to get brief information
  def self.analyze_movie_page
    if @@analysis_queue_movie_page.empty?
      #puts "THREAD_EMPTY: #{@@analyzing_movie_threads}"
      return false
    elsif @@analyzing_movie_threads >= @@MAX_ANALYZING_THREADS
      #puts "THREAD_FULL: #{@@analyzing_movie_threads}"
      sleep(rand / 10)
      return true
    end
    #puts "THREAD_SIZE: #{@@analyzing_movie_threads}"

    analyzing_thread = Thread.new do
      movie_file_path = @@analysis_queue_movie_page.empty? ? nil : @@analysis_queue_movie_page.shift
      if movie_file_path == nil
        # @@analyzing_movie_threads -= 1
        sleep 0.5
        Thread.exit
      end
      #puts "analyzing #{movie_file_path} ..."
      #$stdout.flush
      @@mutex.lock
      @@analyzing_movie_threads += 1
      @@mutex.unlock

      # try to read the movie_page file
      movie_page = nil
      begin
        File.open movie_file_path, "r" do |file|
          movie_page = Nokogiri::HTML(file)
        end
      rescue => ex
        sleep(rand / 20)
        retry
      end

      movie = Hash.new('')

      # get the TMDB_ID from the path
      movie_file_path.split('/')[-1] =~ /(\d+)_/
      movie[:tmdb_id] = $1

      # try to get the main title of the movie
      title_html = movie_page.at_css 'html body div#container div#movie div#mainCol div.title h2#title a span' 
      if title_html == nil
        @@mutex.lock
        @@analyzing_movie_threads -= 1
        @@mutex.unlock
        other_lang_url_html = movie_page.at_css 'html body div#container div#movie.new div#mainCol.new div.carton div.content ul li a' 
        if other_lang_url_html != nil && other_lang_url_html['href'] != nil
          other_lang_thread = Thread.new do
            other_lang_file_path = download_movie_page other_lang_url_html['href'], 'non-en'
            @@analysis_queue_movie_page << other_lang_file_path if other_lang_file_path != nil
          end
        else
          save_to_log("Warning[#{movie[:tmdb_id]}]: Not enough information, ignore ...")
        end
        #sleep 0.5 
        Thread.current.exit
      end
      movie[:title] = title_html.content


      # try to get the overview of the movie
      begin
        movie[:overview] = movie_page.at_css('p#overview').content
      rescue
        # save_to_log("Fail[#{movie[:tmdb_id]}]: #{ex}")
      end    

      # try to get the Tagline of the movie
      begin
        movie[:tagline] = movie_page.at_css('p#tagline').content
      rescue
        # save_to_log("Fail[#{movie[:tmdb_id]}]: #{ex}")
      end

      # try to get the year of release of the movie
      begin
        movie[:year] = movie_page.at_css('h3#year').content.gsub(/\(|\)/, '')
      rescue
        # save_to_log("Fail[#{movie[:tmdb_id]}]: #{ex}")
      end

      # try to get the rating_hint of the movie
      begin
        movie[:rating_hint] = movie_page.at_css('span#rating_hint').content
      rescue
        # save_to_log("Fail[#{movie[:tmdb_id]}]: #{ex}")
      end

      # try to get the director and writers of the movie
      begin
        movie_page.at_css('table.crewStub').css('tr').each do |row|
          if row.at_css('td.job').content =~ /Director/i
            movie[:directors] = row.at_css('td.person').content
          elsif row.at_css('td.job').content =~ /Writers/i
            writers = []
            row.at_css('td.person').content.split(',').each do |writer|
              writers << writer.strip
            end
            movie[:writers] = writers
          end
        end
      rescue
        # save_to_log("Fail[#{movie[:tmdb_id]}]: #{ex}")
      end

      # try to get main_cast of the movie
      begin
        # casts = Hash.new
        casts = []
        movie_page.at_css('table#castTable tbody tr td').css('div.castItem').each do |cast_item|
          actor = cast_item.at_css('span.text p a span span').content
          casts << actor
          # cast_tuple = cast_item.at_css('span.text p')
          # cast_tuple.to_s =~ /<br>\s*?as\s+(.*?)\s*?<\/p>/m
          # cast = $1.split(/\/|\(/)[0].strip
          # casts[actor] = cast
        end
        movie[:top_casts] = casts
      rescue
        # save_to_log("Fail[#{movie[:tmdb_id]}]: #{ex}")
      end

      # try to get metadata of the movie
      begin
        left_column = movie_page.at_css 'html body div#container div#movie div#leftCol'
        left_column.css('p').each do |p|
          if p.at_css('strong') != nil && p.at_css('span') != nil && p.at_css('span').content != '-'
            catelog = p.at_css('strong').content.sub(':', '').downcase.to_sym
            if catelog == :languages
              content = p.at_css('span').content.split(/,\s*/)
            elsif catelog == :webpage
              content = p.at_css('span a')['href']
            else
              content = p.at_css('span').content
            end
            movie[catelog] = content
          end
        end
        # try to get alternative titles of the movie
        left_column.to_s =~ /^<h3>.*?title="View Alternative Titles".*?<ul>(.*?)<\/ul>/m
        alternative_titles_table = $1
        alternative_titles = []
        alternative_titles_table.each_line do |line|
          if line =~ /<li>(.*?)<\/li>/
            alternative_titles << $1
          end
        end
        movie[:alternative_titles] = alternative_titles
      rescue => ex
        # save_to_log("Fail[#{movie[:tmdb_id]}]: #{ex}")
      end

      # try to get the keywords of the movie
      begin
        keywords = []
        keywords_table =  movie_page.at_css('ul.keywords').css("li a span").each do |keyword|
          keywords << keyword.content
        end
        movie[:keywords] = keywords
      rescue
        # save_to_log("Fail[#{movie[:tmdb_id]}]: #{ex}")
      end

      # try to get poster_url of the movie
      begin
        if movie_page.at_css('img#upload_poster')['src'] != 'https://assets.tmdb.org/assets/f996aa2014d2ffddfda8463c479898a3/images/no-poster-w185.jpg'
          movie[:poster] = movie_page.at_css('img#upload_poster')['src']
        end
      rescue
        # save_to_log("Fail[#{movie[:tmdb_id]}]: #{ex}")
      end

      # puts movie.inspect

      # download crew and cast page
      begin
        crew_url = movie_page.at_css('html body div#container div#movie div#mainCol p.more a')['href']
      rescue
        crew_url = nil
      end

      # try to get the crew_url
      if crew_url != nil
        retry_cast_count = 0
        begin
          crew_page = get crew_url
          crew_page = Nokogiri::HTML(crew_page)
          cast_table = crew_page.at_css('table#castTable')
          casts = Hash.new
          cast_table.css('tr').each do |row|
            person = row.at_css('td.person').content
            character = row.at_css('td.character span#cast-entry-').content
            casts[person] = character
          end
          movie[:casts] = casts
        rescue  Net::OpenTimeout, Net::ReadTimeout => ex
          retry_cast_count += 1
          sleep(Random.rand / 10)
          retry unless retry_cast_count > 5
          save_to_log("Fail[Download Casts of #{movie[:tmdb_id]}]: download #{base_uri}#{crew_url} - #{ex}")
        rescue => ex
          # save_to_log("Fail[Analyze Casts of #{movie[:tmdb_id]}]: #{base_uri}#{crew_url} - #{ex}")
        end
      end

      # download reviews page named by ID_reviews.html
      begin
        # TODO: Pending to complete if needed
      rescue Net::OpenTimeout
        timeout_try += 1
        retry unless timeout_try > 2
      rescue => ex
        save_to_log("Fail[Download Reviews #{movie[:tmdb_id]}]: #{base_uri}#{crew_url} - #{ex}")
      end

      # call analyze_reviews to get information about reviews
      # TODO: Pending to complete if needed

      # call analyze_recommandation to get information about recommandation
      # TODO: Pending to complete if needed

      # try to write the info of the movie to #{MOVIES_DB_FILE}
      retry_count = 0
      begin
        file = File.new(@@MOVIES_DB_FILE, "a")
        file.flock(File::LOCK_EX)
        file.puts movie.to_json + ','
        file.flock(File::LOCK_UN)
        #rescue RuntimeError => ex
        #  save_to_log("Fail: #{ex} [#{Time.now}]")
      rescue
        sleep(rand / 10)
        retry
      end
      @@mutex.lock
      @@analyzing_movie_threads -=1
      @@TOTAL_MOVIES += 1
      @@mutex.unlock
      #sleep 0.5
    end
    return true
  end

  def self.save_to_log(message)
    begin
      File.open @@LogFile, "a" do |file|
        file.flock(File::LOCK_EX)
        file.write(message + "\n")
        file.flock(File::LOCK_UN)
      end
    rescue
      sleep(Random.rand / 10)
      retry
    end
  end
end

# OpenSSL::SSL::VERIFY_PEER = OpenSSL::SSL::VERIFY_NONE
TMDbCrawler.begin_crawl_movies



