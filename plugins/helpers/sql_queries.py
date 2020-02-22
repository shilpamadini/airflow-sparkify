class SqlQueries:
    songplay_table_insert = ("""
        insert into songplays (start_time,level,user_id,song_id,artist_id,session_id,location,user_agent)
(select distinct (TIMESTAMP 'epoch' + s.ts/1000 * interval '1 second') AS start_time,
s.level,
s.userid,
songs.song_id,
songs.artist_id,
s.sessionid::int,
s.location,
s.useragent
from staging_events s , songs, artists
where s.song = songs.title
and s.length = songs.duration
and artists.name = s.artist
and songs.artist_id = artists.artist_id
and s.page = 'NextSong'
)
""")

    user_table_insert = ("""
        insert into users (select distinct userid,firstName,lastName,gender,level \
from staging_events where userid is not null)
    """)

    song_table_insert = ("""
        insert into songs (select distinct song_id, title,artist_id,year::int as year, \
duration::float8 as duration from staging_songs)
    """)

    artist_table_insert = ("""
        insert into artists (select distinct
artist_id,artist_name,artist_location,artist_latitude::float,artist_logitude::float from staging_songs)
    """)

    time_table_insert = ("""
        insert into time (SELECT DISTINCT (TIMESTAMP 'epoch' + ts/1000 * interval '1 second') AS start_time,
date_part(h, start_time)::int as hour,
date_part(d, start_time)::int as day,
date_part(w, start_time)::int as week,
date_part(mon, start_time)::int as month,
date_part(y, start_time)::int as year,
date_part(dow, start_time)::int as weekday
FROM staging_events )
    """)
