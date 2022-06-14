# Bismuth Map - Worldwide Nodes
This is a map of worldwide Bismuth nodes.

 This app needs a mapbox token  Runs on port 5000.
 
 - Start by running **map.py**

 To run it on heroku:
 > web: gunicorn bismap:bismap
 
 > worker: celery -A bismap.celery worker

 ## About Bismuth Map
 Introduction: https://hypernodes.bismuth.live/?p=2561
 Created by: Dingo
 Example URL: https://bismuth-map.herokuapp.com/
