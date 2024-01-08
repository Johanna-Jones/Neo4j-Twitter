// RETWEETS

CALL apoc.periodic.iterate(
'CALL apoc.load.json("file:///Fulltweets.json") YIELD value' ,
'WITH
value.id AS id,
value.object.id as RTid, // subtweet id
datetime({ epochMillis: apoc.date.parse(value.postedTime, "ms",
"yyyy-MM-dd\'T\'HH:mm:ss.SSS\'Z\'")}) AS postedTimestamp ,
value.text AS text,
value.twitter_lang AS language ,
value.retweetCount AS retweetCount ,
value.favoritesCount AS favoritesCount,
value.verb as verb,
value.twitter_entities.user_mentions AS mentions,
value.twitter_entities.hashtags AS hashtags,
value.twitter_entities.urls AS urls
where value.verb = "share"
MERGE (RT:Tweet{RTid:RTid, id: id})    
ON CREATE SET
RT.postedTimestamp = postedTimestamp,
RT.text = text,
RT.language = language ,
RT.retweetCount = retweetCount ,
RT.favoritesCount = favoritesCount,
RT.shared_tweet = "retweet" // custom retweet flag
', 
{batchSize:500}) YIELD * ;


// hashtags
CALL apoc.periodic.iterate(
'CALL apoc.load.json("file:///Fulltweets.json") YIELD value' ,
'WITH
value.id as id,
value.verb as verb,
value.twitter_entities as entities
where value.verb = "share"
unwind entities.hashtags as hashtags
unwind hashtags.text as text
match (t:Tweet{id:id})

FOREACH (hashtagData IN hashtags |
  MERGE (h:Hashtag {tag: hashtagData.text})
  MERGE (t)-[:TAGS]->(h)
)

',

{batchSize:500}) YIELD * ;


// links

CALL apoc.periodic.iterate(
'CALL apoc.load.json("file:///Fulltweets.json") YIELD value' ,
'WITH
value.id as id,
value.verb as verb,
value.twitter_entities.hashtags AS hashtags,
value.twitter_entities.urls AS urls
where value.verb = "share"
match (t:Tweet{id:id})

FOREACH (urlData IN urls |
  MERGE (l:Link {url: urlData.expanded_url})
  MERGE (t)-[:CONTAINS]->(l)
)

FOREACH (hashtagData IN hashtags |
  MERGE (h:Hashtag {tag: hashtagData.text})
  MERGE (t)-[:TAGS]->(h)
)
',

{batchSize:500}) YIELD * ;




// Source node
CALL apoc.periodic.iterate(
'CALL apoc.load.json("file:///Fulltweets.json") YIELD value' ,
'WITH
value.id as id,
value.generator as generators,
value.verb AS verb,  // Extract the verb
value.twitter_entities.hashtags AS hashtags,
value.twitter_entities.urls AS urls 
where value.verb = "share"
match (t:Tweet{id:id})


 FOREACH (gen IN generators |
    MERGE (s:Source {source_name: gen.displayName, source_link: gen.link})
    MERGE (t)-[:USING]->(s)
)

',

{batchSize:500}) YIELD * ;

// user node
CALL apoc.periodic.iterate(
'CALL apoc.load.json("file:///Fulltweets.json") YIELD value' ,
'WITH
value.id as id,
value.verb as verb,
value.actor as actor,
value.twitter_entities.user_mentions as mentions
where value.verb = "share"
match (t:Tweet{id:id})
merge (u:Users{id:actor.id})
ON CREATE SET
u.username = actor.preferredUsername,
u.displayname = actor.displayName

MERGE (u) -[:POSTS]->(t)

FOREACH (mention in mentions |
MERGE (m: Users{id: mention.id_str, screen_name : mention.screen_name})
MERGE (t)-[:MENTIONS]->(m)

)

', 
{batchSize:500}) YIELD * ;


------------------------------------------------------------------------

///TWEETS 

CALL apoc.periodic.iterate(
'CALL apoc.load.json("file:///Fulltweets.json") YIELD value' ,
'WITH
value.id AS id,
datetime({ epochMillis: apoc.date.parse(value.postedTime, "ms",
"yyyy-MM-dd\'T\'HH:mm:ss.SSS\'Z\'")}) AS postedTimestamp ,
value.text AS text,
value.twitter_lang AS language ,
value.retweetCount AS retweetCount ,
value.favoritesCount AS favoritesCount,
value.verb as verb,
value.twitter_entities.user_mentions AS mentions,
value.twitter_entities.hashtags AS hashtags,
value.twitter_entities.urls AS urls
where value.verb = "post"
MERGE (RT:Tweet{id:id})    
ON CREATE SET
RT.postedTimestamp = postedTimestamp,
RT.text = text,
RT.language = language ,
RT.retweetCount = retweetCount ,
RT.favoritesCount = favoritesCount,
RT.shared_tweet = "post" // custom retweet flag
', 
{batchSize:500}) YIELD * ;


// hashtags
CALL apoc.periodic.iterate(
'CALL apoc.load.json("file:///Fulltweets.json") YIELD value' ,
'WITH
value.id as id,
value.verb as verb,
value.twitter_entities as entities
where value.verb = "post"
unwind entities.hashtags as hashtags
unwind hashtags.text as text
match (t:Tweet{id:id})

FOREACH (hashtagData IN hashtags |
  MERGE (h:Hashtag {tag: hashtagData.text})
  MERGE (t)-[:TAGS]->(h)
)

',

{batchSize:500}) YIELD * ;


// links

CALL apoc.periodic.iterate(
'CALL apoc.load.json("file:///Fulltweets.json") YIELD value' ,
'WITH
value.id as id,
value.verb as verb,
value.twitter_entities.hashtags AS hashtags,
value.twitter_entities.urls AS urls
where value.verb = "post"
match (t:Tweet{id:id})

FOREACH (urlData IN urls |
  MERGE (l:Link {url: urlData.expanded_url})
  MERGE (t)-[:CONTAINS]->(l)
)

FOREACH (hashtagData IN hashtags |
  MERGE (h:Hashtag {tag: hashtagData.text})
  MERGE (t)-[:TAGS]->(h)
)
',

{batchSize:500}) YIELD * ;




// Source node
CALL apoc.periodic.iterate(
'CALL apoc.load.json("file:///Fulltweets.json") YIELD value' ,
'WITH
value.id as id,
value.generator as generators,
value.verb AS verb,  // Extract the verb
value.twitter_entities.hashtags AS hashtags,
value.twitter_entities.urls AS urls 
where value.verb = "post"
match (t:Tweet{id:id})


 FOREACH (gen IN generators |
    MERGE (s:Source {source_name: gen.displayName, source_link: gen.link})
    MERGE (t)-[:USING]->(s)
)

',

{batchSize:500}) YIELD * ;

// user node
CALL apoc.periodic.iterate(
'CALL apoc.load.json("file:///Fulltweets.json") YIELD value' ,
'WITH
value.id as id,
value.verb as verb,
value.actor as actor,
value.twitter_entities.user_mentions as mentions
where value.verb = "post"
match (t:Tweet{id:id})
merge (u:Users{id:actor.id})
ON CREATE SET
u.username = actor.preferredUsername,
u.displayname = actor.displayName

MERGE (u) -[:POSTS]->(t)

FOREACH (mention in mentions |
MERGE (m: Users{id: mention.id_str, screen_name : mention.screen_name})
MERGE (t)-[:MENTIONS]->(m)

)

', 
{batchSize:500}) YIELD * ;

-----------------------------



///// SUBTEWEETs

CALL apoc.periodic.iterate(
'CALL apoc.load.json("file:///Fulltweets.json") YIELD value' ,
'WITH


//subtweet information
value.object.id as id, // subtweet id which matches with RTID from retweets
datetime({ epochMillis: apoc.date.parse(value.object.postedTime, "ms",
"yyyy-MM-dd\'T\'HH:mm:ss.SSS\'Z\'")}) AS postedTimestamp ,
value.object.text AS text,
value.object.twitter_lang AS language ,
value.object.retweetCount AS retweetCount ,
value.object.favoritesCount AS favoritesCount,
value.verb as verb,
value.object.twitter_entities.user_mentions AS mentions,
value.object.twitter_entities.hashtags AS hashtags,
value.object.twitter_entities.urls AS urls
where value.verb = "share"
match (RT:Tweet{RTid: id})
MERGE (t:Tweet{id:id})    
ON CREATE SET
t.postedTimestamp = postedTimestamp,
t.text = text,
t.language = language ,
t.retweetCount = retweetCount ,
t.favoritesCount = favoritesCount,
t.shared_tweet = "subtweet" // custom retweet flag


merge (RT) -[:RETWEET_OF]->(t)
', 
{batchSize:500}) YIELD * ;



// hashtags
CALL apoc.periodic.iterate(
'CALL apoc.load.json("file:///Fulltweets.json") YIELD value' ,
'WITH
value.object.id as id,// subtweet id
value.verb as verb,
value.object.twitter_entities as entities
where value.verb = "share"
unwind entities.hashtags as hashtags
unwind hashtags.text as text
match (t:Tweet{id:id}) // matches to the subtweet id

FOREACH (hashtagData IN hashtags |
  MERGE (h:Hashtag {tag: hashtagData.text})
  MERGE (t)-[:TAGS]->(h)
)

',

{batchSize:500}) YIELD * ;


// links

CALL apoc.periodic.iterate(
'CALL apoc.load.json("file:///Fulltweets.json") YIELD value' ,
'WITH
value.object.id as id, //subtweet id
value.verb as verb,
value.object.twitter_entities.hashtags AS hashtags,
value.object.twitter_entities.urls AS urls
where value.verb = "share"
match (t:Tweet{id:id})

FOREACH (urlData IN urls |
  MERGE (l:Link {url: urlData.expanded_url})
  MERGE (t)-[:CONTAINS]->(l)
)

FOREACH (hashtagData IN hashtags |
  MERGE (h:Hashtag {tag: hashtagData.text})
  MERGE (t)-[:TAGS]->(h)
)
',

{batchSize:500}) YIELD * ;


// Source node
CALL apoc.periodic.iterate(
'CALL apoc.load.json("file:///Fulltweets.json") YIELD value' ,
'WITH
value.object.id as id, // subtweet id
value.object.generator as generators,
value.verb AS verb,  // Extract the verb
value.object.twitter_entities.hashtags AS hashtags,
value.object.twitter_entities.urls AS urls 
where value.verb = "share"
match (t:Tweet{id:id})


 FOREACH (gen IN generators |
    MERGE (s:Source {source_name: gen.displayName, source_link: gen.link})
    MERGE (t)-[:USING]->(s)
)

',

{batchSize:500}) YIELD * ;




// user node
CALL apoc.periodic.iterate(
'CALL apoc.load.json("file:///Fulltweets.json") YIELD value' ,
'WITH
value.object.id as id,
value.verb as verb,
value.object.actor as actor,
value.object.twitter_entities.user_mentions as mentions
where value.verb = "share"
match (t:Tweet{id:id})
merge (u:Users{id:actor.id})
ON CREATE SET
u.username = actor.preferredUsername,
u.displayname = actor.displayName

MERGE (u) -[:POSTS]->(t)

FOREACH (mention in mentions |
MERGE (m: Users{id: mention.id_str, screen_name : mention.screen_name})
MERGE (t)-[:MENTIONS]->(m)

)

', 
{batchSize:500}) YIELD * ;



