
//Question 1
match (s:Source) <-[:USING]- (t:Tweet) <-[:POSTS]- (u:Users)
with s.source_name as source, 
        count(t) as num_posts, 
     collect(distinct(u.username)) as num_users
return source, num_posts, size(num_users) as num_users
order by num_posts DESC, num_users DESC
LIMIT 5

// Question 2
match (h:Hashtag) <-[:TAGS]- (t:Tweet) <-[:POSTS]- (u:Users)
where t.retweetCount >50
with u.username as username,
    count(t) as top_posts,
    collect(h.tag) as h_text // puts hashtags in a list, allows for duplicates
with username, top_posts, h_text, apoc.coll.frequencies(h_text) AS tagCounts // finds the frequencies of hashtags
with username, top_posts, h_text,apoc.coll.sortMaps(tagCounts, 'value DESC') as sortedTagCounts // sorts the tags by count
unwind  sortedTagCounts as tags
return username,
        top_posts as num_pop_posts, 
        collect(tags.item)[0..2] as top_hashtags // retrieve first and second index places
order by top_posts DESC
limit 3

// 3

MATCH p=shortestPath((u:Users{username: 'luckyinsivan'}) -[*]- (h:Hashtag{tag:'imsosick'}))
WHERE NONE(rel IN relationships(p) WHERE type(rel) = 'USING')
RETURN p



