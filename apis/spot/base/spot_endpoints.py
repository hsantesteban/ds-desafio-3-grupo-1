from enum import Enum, unique

# docs: https://developer.spotify.com/documentation/web-api/reference


@unique
class SpotifyTrackEndpoints(Enum):
    # /id
    GET_TRACK = 'https://api.spotify.com/v1/tracks/{id}'
    # ?ids ?market
    GET_SEVERAL_TRACKS = 'https://api.spotify.com/v1/tracks'
    # /id
    GET_TRACK_AUDIO_FEATURES = 'https://api.spotify.com/v1/audio-features/{id}'
    # ?ids
    GET_SEVERAL_TRACKS_AUDIO_FEATURES = 'https://api.spotify.com/v1/audio-features'
    # /id
    GET_TRACK_AUDIO_ANALYSIS = 'https://api.spotify.com/v1/audio-analysis/{id}'


@unique
class SpotifyArtistsEndpoints(Enum):
    # /id
    GET_ARTIST = 'https://api.spotify.com/v1/artists/{id}'
    # ?ids
    GET_SEVERAL_ARTISTS = 'https://api.spotify.com/v1/artists'
    # ?market
    GET_TOP_TRACKS = 'https://api.spotify.com/v1/artists/{id}/top-tracks'
    # ?include_groups ?market ?limit ?offset
    GET_ALBUMS = 'https://api.spotify.com/v1/artists/{id}/albums'


@unique
class SpotifyAlbumsEndpoints(Enum):
    # /id
    GET_ALBUM = 'https://api.spotify.com/v1/albums/{id}'
    # ?ids ?market
    GET_SEVERAL_ALBUMS = 'https://api.spotify.com/v1/albums'
    # ?market ?limit ?offset
    GET_ALBUM_TRACKS = 'https://api.spotify.com/v1/albums/{id}/tracks'
