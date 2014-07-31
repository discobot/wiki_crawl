domain = "http://simple.wikipedia.org"
start_page = "http://simple.wikipedia.org/wiki/Main_Page"
article_pattern = domain + "/wiki/([\w\(\)]+)"

article_crawlers = 10
utility_crawlers = 5

wiki_utility_pages = ["/wiki/" + type for type in ["Talk:", "Category:", "Help:", "Special:", 
                                     "Wikipedia:", "Wikipedia_talk:", "Template:", "T:", "User:", "User_talk", "User_talk"]]
wiki_ignored_pages = ["/wiki/File:", "index.php", "query.php", "api.php", "pagename"]

ban_prefix = ["mailto"]
ban_sufix = [".jpeg", ".jpg", ".tiff", ".png", ".ocr", ".mov"]
 
