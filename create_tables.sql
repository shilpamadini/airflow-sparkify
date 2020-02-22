staging_events_table_create= ("CREATE TABLE IF NOT EXISTS staging_events(
                           artist text,
                           auth text,
                           firstName text,
                           gender text,
                           itemInSession text,
                           lastName text,
                           length text,
                           level text,
                           location text,
                           method text,
                           page text,
                           registration text,
                           sessionId text,
                           song text,
                           status text,
                           ts bigint,
                           userAgent text,
                           userId text
                            );")


staging_songs_table_create = ("CREATE TABLE IF NOT EXISTS staging_songs(
                           num_songs text,
                           artist_id text,
                           artist_latitude text,
                           artist_logitude text,
                           artist_location text,
                           artist_name text,
                           song_id text,
                           title text,
                           duration text,
                           year text
                            );")

songplay_table_create = ("CREATE TABLE IF NOT EXISTS songplays(
                           songplay_id int IDENTITY not null,
                           start_time timestamp,
                           level varchar,
                           user_id varchar,
                           song_id varchar,
                           artist_id varchar,
                           session_id int,
                           location text,
                           user_agent text,
                           primary key(songplay_id),
                           foreign key(user_id) references users(user_id),
                           foreign key(song_id) references songs(song_id),
                           foreign key(artist_id) references artists(artist_id),
                           foreign key(start_time) references time(start_time))
                           distkey(artist_id) sortkey(start_time);")

user_table_create = ("CREATE TABLE IF NOT EXISTS users(
                       user_id varchar not null primary key,
                       first_name varchar,
                       last_name varchar,
                       gender varchar,
                       level varchar
                    ) diststyle all;")

song_table_create = ("CREATE TABLE IF NOT EXISTS songs(
                       song_id varchar not null,
                       title varchar ,
                       artist_id varchar ,
                       year int,
                       duration float8,
                       primary key(song_id),
                       foreign key(artist_id) references artists(artist_id)
                      ) distkey(artist_id);")

artist_table_create = ("CREATE TABLE IF NOT EXISTS artists(
                         artist_id varchar not null,
                         name varchar,
                         location varchar,
                         latitude float,
                         longitude float,
                         primary key(artist_id)
                       ) distkey(artist_id);")

time_table_create = ("CREATE TABLE IF NOT EXISTS time(
                       start_time timestamp not null,
                       hour int,
                       day int,
                       week int,
                      month int,
                      year int,
                      weekday int,
                      primary key (start_time)
                     ) diststyle all sortkey(start_time);")
