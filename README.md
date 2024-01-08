# games-scraper

Extract games data from www.metacritic.com.

Result file contains data in csv format and has fields listed below.

Common field:
- link
- name
- developer
- publisher
- summary
- genres
- rating

Platform specific fields:
- platform
- release_date
- metascore
- critic_reviews_count
- positive_critic_reviews_count
- mixed_critic_reviews_count
- negative_critic_reviews_count
- user_score
- user_reviews_count
- positive_user_reviews_count
- mixed_user_reviews_count
- negative_user_reviews_count

[The dataset on Kaggle](https://www.kaggle.com/datasets/darianogina/best-video-games-of-all-time-metacritic)

## Technology stack
- selectolax -- to extract data from pages
- hrequests -- to avoid to be blocked
- chompjs -- to convert json string to normal json
