#!/usr/bin/ruby
# Code programmed by JIN, Yue

require 'nokogiri'
require 'open-uri'
require 'json'

$all_actors = Hash.new
all_films = []

$Actor_no = 0

def Actor_Crawller(page, index) ##传入当前的film页面和starring所在的位置

    star_infotable = []   
    star_infotable = page.css 'table.infobox.vevent' 

    act_info = []
    act_info = star_infotable[0].css 'tr//td'

    ###获得当前filmpage中所有actor的链接
    actor_url = []
    # url = act_info[index-1].css 'ul//li//a'
    url = act_info[index-1].css 'a'
    url.each do |u|
        actor_url.push u['href']
    end
    for i in 0..(actor_url.length-1)
        tmp = "https://en.wikipedia.org" + actor_url[i]
        actor_url[i] = tmp
        # $all_actors[actor_url[i]] = 1
    end
    # puts actor_url

    ###每一个actor的URL产生一个actor(hash)并存入数组all_actors[]
    actor_url.each do |u|
      if ($all_actors.has_key?(u) == false)
        puts u
        $all_actors[u] = 1

        actor = Hash.new
        # a_page = Nokogiri::HTML(open u)
        begin
            a_page = Nokogiri::HTML(open u)
        rescue OpenURI::HTTPError => e
            puts "******* Cannot Access *******"
            next
        end
        a_infotable = []
        a_infotable = a_page.css 'table.infobox'

        if(a_infotable[0] == nil)
            next
        end

        a_att = []  #记录实体名
        a_attribute = a_infotable[0].css'tr//th'

        a_attribute.each do |a|
            form = a.content
            form.chomp!
            form.strip!        
            form.gsub!(/\n/, ' ')
            form.gsub!(/,/, ';')    
            form.gsub!(/\u00A0/, ' ')
            form.gsub!(/\u2013/, '-')
            # form.gsub!(/Born/, 'birthday')
            form.gsub!(/Relatives/, 'relative')
            form.gsub!(/Occupation/, 'gender')
            form.gsub!(/Other names/, 'alias')
            form.gsub!(/\(s+\)/, '')
            # form.gsub!(/Years Active/, 'years_active')
            # form.gsub!(/Nationality/, 'nationality')
            # form.gsub!(/Known for/, 'known_for')
            form.gsub!(/ /, '_')
            form.downcase!

            a_att.push form

        end

        actor['name'] = a_att[0].capitalize
        actor['name'] = actor['name'].gsub(/_/, ' ')

        a_info = [] #记录实体对应信息
        a_information = a_infotable[0].css'tr//td'
        # a_bp = "null"
        a_birthplace = []
        a_birthplace = a_infotable[0].css'tr//td//span.birthplace'
        if(a_birthplace[0] != nil)
            actor['place_of_birth'] = a_birthplace[0].content
        end   

        a_birthday = []
        a_birthday = a_infotable[0].css'tr//td//span.bday'
        if(a_birthday[0] != nil)
            actor['birthday'] = a_birthday[0].content
        end

        a_information.each do |c|
            form = c.content
            form.chomp!
            form.strip!
            form.gsub!(/\n/, ';')
            form.gsub!(/,/, ';')
            form.gsub!(/\u00A0/, ' ')
            form.gsub!(/\u2013/, '-')

            a_info.push form
        end
        # puts a_info
    
        
        # actor['place_of_birth'] = a_bp
        if (a_info.length == a_att.length)
            for k in 1..10
                actor[a_att[k]] = a_info[k]
                # if(a_att[k] == 'birthday')
                    # actor['birthday'] = actor['birthday'].slice(/\d{4}-\d{2}-\d{2}/)

                if(a_att[k] == 'gender')
                    if(actor[a_att[k]][0..4] == 'Actor')
                        actor[a_att[k]] = 'M'
                    else
                        actor[a_att[k]] = 'F'
                    end
                end

                if(a_att[k] == "known_for" || a_att[k] == "nationality" || a_att[k] == "education" || a_att[k] == "spouse" || a_att[k] == "partner")
                    actor[a_att[k]] = actor[a_att[k]].split(';')
                end
            end
        else
            for k in 1..10
                actor[a_att[k]] = a_info[k-1]
                # if(a_att[k] == 'birthday')
                    # actor['birthday'] = actor['birthday'].slice(/\d{4}-\d{2}-\d{2}/)
                if(a_att[k] == 'gender')
                    if(actor[a_att[k]][0..4] == 'Actor')
                        actor[a_att[k]] = 'M'
                    else
                        actor[a_att[k]] = 'F'
                    end
                end

                if(a_att[k] == "known_for" || a_att[k] == "nationality" || a_att[k] == "education" || a_att[k] == "spouse" || a_att[k] == "partner")
                    # puts a_att[k]
                    # puts actor[a_att[k]]
                    actor[a_att[k]] = actor[a_att[k]].split(';')
                end
            end
        end
        # puts actor
        # all_actors.push actor
        file_path = '/Users/Crystal/Desktop/Data_Portal/6000D/actor0/'+ u[30, u.length-1]+'.json'
        # puts $Actor_no
        # file_path = '/Users/Crystal/Desktop/Data_Portal/6000D/actor/'+ $Actor_no.to_s + '.json'
        File.open(file_path, 'w') do |file|
            file.puts actor.to_json
        end
        # $Actor_no += 1
      end
    end    
end 
    
# puts 'begin download'
page0 = Nokogiri::HTML(open "https://en.wikipedia.org/wiki/Lists_of_films")
# puts 'download complete'
###定位年份
f_year = page0.css 'div#mw-content-text.mw-content-ltr//div.hlist//dl//dd//a'

###得到每个年份的url
year_page = []
f_year.each do |fy|
    tmp = fy['href']
    y_url = "https://en.wikipedia.org" + tmp
    year_page.push y_url
end

###打开年份网页，抓到电影名字及其url
film_url = []
#year_page.each do |yearurl|
for i in 0..28
    # puts i
# while(i < year_page.length)
    page1 = Nokogiri::HTML(open year_page[i]);
    location = []

    # if(i<29)
        #page1 = Nokogiri::HTML(open year_page[i]);
        location = page1.css 'table.wikitable'
        len = location.length

        for j in (len-4)..(len-1)
            f_name = location[j].css 'td//i//a'
            f_name.each do |fn|    
                tmp = fn['href']
                f_url = "https://en.wikipedia.org" + tmp
                film_url.push f_url
            end
        end 

    # else
    #     table = page1.css
    # end

    # i = i+1
end
# puts film_url.length

###爬取每个网页的信息，写成film(hash)，存入all_films
# film_url.each do |filmpage|
# a_no = 0
for j in (2204..film_url.length-1)

    # puts 
    
    film = Hash.new
    size = 20

    begin
        # page2 = Nokogiri::HTML(open filmpage)
        page2 = Nokogiri::HTML(open film_url[j])
    rescue OpenURI::HTTPError => e
        puts "********** Cannot Access *************"
        next
    end
    # puts filmpage
    puts j

    f_infotable = []   
    f_infotable = page2.css 'table.infobox'

    if(f_infotable[0] == nil)
        next
    end

    f_att = []  #记录实体名
    f_attribute = f_infotable[0].css'tr//th'
    f_attribute.each do |a|
        form = a.content
        form.chomp!
        form.strip! 
        form.gsub!(/\n/, ' ')
        form.gsub!(/Directed by/, 'directors')
        form.gsub!(/Produced by/, 'producer')
        form.gsub!(/Written by/, 'writers')
        form.gsub!(/Running time/, 'total_time')
        form.gsub!(/Edited by/, 'editors')
        form.gsub!(/Release dates/, 'year')
        form.gsub!(/Language/, 'languages')
        form.gsub!(/ /, '_')
        form.downcase!

        f_att.push form

        if (form == 'starring')
            index = f_att.length
            Actor_Crawller(page2, index)   #对出现的演员做crawlling
            # puts index
        end
    end

    f_info = [] #记录实体对应信息
    f_information = f_infotable[0].css'tr//td'
    f_information.each do |c|
        form = c.content
        form.chomp!
        form.strip!
        form.gsub!(/\n/, ';')
        form.gsub!(/minutes/, '')
        form.gsub!(/\[\d+\]/, '')
        f_info.push form
    end
    #puts info
    
    ###建立当前film(hash)
    film['title'] = f_att[0].capitalize
    film['title'] = film['title'].gsub(/_/, ' ')
    if (f_info.length == f_att.length)
        for k in 1..(size-1)
            film[f_att[k]] = f_info[k]
            if(f_att[k] == 'year')
                if(film['year'].slice(/\d{4}-\d{2}-\d{2}/) == nil)                
                    film['year'] = film['year'].slice(/\d{4}/)
                else
                    film['year'] = film['year'].slice(/\d{4}-\d{2}-\d{2}/)
                    film['year'] = film['year'].slice(/\d{4}/)
                end
            end
            if(f_att[k] == 'writers' || f_att[k] == 'directors' || f_att[k] == 'country' || f_att[k] == 'editors' || f_att[k] == 'starring' || f_att[k] == 'production_company' || f_att[k] == 'languages') 
                film[f_att[k]] = film[f_att[k]].split(';')
            end 
        end
    else
        for k in 1..(size-1)
           film[f_att[k]] = f_info[k-1]
            if(f_att[k] == 'year')
                if(film['year'].slice(/\d{4}-\d{2}-\d{2}/) == nil)                
                    film['year'] = film['year'].slice(/\d{4}/)
                else
                    film['year'] = film['year'].slice(/\d{4}-\d{2}-\d{2}/)
                    film['year'] = film['year'].slice(/\d{4}/)
                end
            end

            if(f_att[k] == 'writers' || f_att[k] == 'directors' || f_att[k] == 'country' || f_att[k] == 'editors' || f_att[k] == 'starring' || f_att[k] == 'production_company' || f_att[k] == 'languages') 
                film[f_att[k]] = film[f_att[k]].split(';')
            end 
        end
    end 

   
    # puts film
    # all_films.push film
    # file_path = '/Users/Crystal/Desktop/Data_Portal/6000D/film/'+ film_url[j][30, film_url.length-1]+'.json'
    file_path = '/Users/Crystal/Desktop/Data_Portal/6000D/film0/'+ j.to_s + '.json'
    File.open(file_path, 'w') do |file|
        file.puts film.to_json
    end 
    j += 1

end

# puts all_films
# puts "**********"
# puts all_actors

# file_path = '/Users/Crystal/Desktop/film_data.json'
# File.open(file_path, 'w') do |file|
#     file.puts all_films.to_json
# end

# file_path = '/Users/Crystal/Desktop/actor_data.json'
# File.open(file_path, 'w') do |file|
#     file.puts all_actors.to_json
# end




