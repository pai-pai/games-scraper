# games-scraper

Extract data about games from www.metacritic.com.

Result file contains data in csv format and has fields listed below:
- link
- name
- release_date
- developer
- platform
- additional_platforms
- summary
- genres
- number_of_players
- rating
- metascore
- number_of_critic_reviews
- user_score
- number_of_user_ratings
- awards_and_ranking

[The dataset on Kaggle](https://www.kaggle.com/datasets/darianogina/best-video-games-of-all-time-metacritic)

### Game details page

<img src="https://pai-pai-github-images.s3.amazonaws.com/games-scraper-page.png" height="75%" width="75%" alt="Collected data" />

### Collected games

<img src="https://pai-pai-github-images.s3.amazonaws.com/games-scraper-result.png" height="75%" width="75%" alt="Collected data" />

## Technology stack
- Beautiful Soup -- to extract data from pages
- requests -- for the 1st step of scraper (put links into intermediate 'games_links' file)
- aiohttp -- for the 2nd step (using data from 'games_links' visit every game details page)
