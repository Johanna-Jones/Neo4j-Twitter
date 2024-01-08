

// Added the domain name to the relationship CONTAINS
match (t:Tweet)-[c:CONTAINS]-> (l:Link)
with t,c,l,split(l.url, "://")[0] as protocol,
     split(l.url, "://")[1] as rest
with t,c,l,split(rest, "/")[0] as domain

set c.domain_name = domain
return t, c, l




// Part A - Query
// Added the domain name to the relationship CONTAINS
match (t:Tweet)-[c:CONTAINS]-> (l:Link)
with t,c,l,split(l.url, "://")[0] as protocol,
     split(l.url, "://")[1] as rest
with t,c,l,split(rest, "/")[0] as domain

set c.domain_name = domain
return t, c, l

// writing the query to find users that post from realestate
match (l:Link) <-[c:CONTAINS]- (t:Tweet)<-[:POSTS]- (u:Users)
with l, t, c, collect(distinct(u.username)) as username
where c.domain_name in [ 'www.realestate.com.au', 'realestate.com.au', 'realestate.com']
return c.domain_name,username, count(t)

// remove the domain name property
match (l:Link) <-[c:CONTAINS]- (t:Tweet)
remove c.domain_name



// Part B- Implementing domain node

match (l:Link)<-[:CONTAINS]- (t:Tweet)
with l,t,split(l.url, "://")[0] as protocol,
     split(l.url, "://")[1] as rest
with l,t,split(rest, "/")[0] as domain
with l,t,collect(domain) as domains
foreach( dom in domains|
merge (d:Domain{domain_name : dom})
merge (d)-[:LINK_FROM]->(l)
merge (t)-[:FROM_DOMAIN]->(d)

)
// Implementing a Text node index
// Improves query performance
CREATE TEXT INDEX domain_index IF NOT EXISTS FOR (d:Domain) on (d.domain_name)




// Part C

// Querying  for top posts in LinkedIn.
match (u:Users)-[:POSTS]->(t:Tweet)-[:FROM_DOMAIN]-> (d:Domain)
where d.domain_name IN ["www.linkedin.com","lnkd.in"]
with  u.username as username,
    count(t) as posts
order by posts desc

return collect(username) as user_names, posts as num_posts
limit 1



// Part D - Creating solution to accomodate other Organisations and Industries.

match (d:Domain)
merge (o:Organisation{orgid:"id"})
ON CREATE set 
o.name = "Organisation",
o.industry = "Industry"
merge (d)-[:PART_OF]->(o)


match (o:Organisation)
merge (i:Industry{industry: "Industry"})
ON CREATE SET
i.subindustry= "Subindustry",
i.experience= "[entry level, senior level, director, partner]",
i.base_pay = "[salary ranges]"

merge (i)-[:BELONGS_TO]-> (o)