# 🛰 Overview
As a Senior Engineer at Bright New Company, you’ll be designing geospatial microservices using modern
Python frameworks and scalable data backends. This assignment simulates a real-world task
using traffic speed data to provide spatial insights through an API and visualization.
🎯 Your Objective
Build a FastAPI microservice that:
1. Ingests and stores geospatial traffic datasets using PostgreSQL + PostGIS
2. Uses SQLAlchemy ORM models for all data interactions
3. Implements RESTful API endpoints for spatial and temporal aggregation
4. Visualizes results using MapboxGL in a Jupyter Notebook (Example Below)
5. Presents an architecture diagram of your solution
📦 Datasets
You will be working with two Parquet datasets:
- ● Link Info Dataset:
https://cdn.urbansdk.com/data-engineering-interview/link_info.parquet.gz
- ● Speed Data:
https://cdn.urbansdk.com/data-engineering-interview/duval_jan1_2024.parquet.gz
-🕒 Time Periods
Use the following periods for time-based aggregation:

- Period 1: 00:00 - 03:59
- Period 2: 04:00 - 06:59
- Period 3: 07:00 - 09:59
- Period 4: 10:00 - 12:59
- Period 5: 13:00 - 15:59
- Period 6: 16:00 - 18:59
- Period 7: 19:00 - 23:59

✅ Required API Endpoints
You must implement the following endpoints in your FastAPI service:
- 1⃣ /aggregates/
Method: GET
Query Params: day, period
Returns:
Aggregated average speed per link for the given day and time period.
- 2⃣ /aggregates/{link_id}
Method: GET
Query Params: day, period
Returns:
Speed and metadata for a single road segment.
- 3⃣ /patterns/slow_links/
Method: GET
Query Params: period, threshold, min_days
Returns:
Links with average speeds below a threshold for at least min_days in a week.
- 4⃣ /aggregates/spatial_filter/
Method: POST
Body Example:
json
CopyEdit
{
"day": "Wednesday",
"period": "AM Peak",
"bbox": [-81.8, 30.1, -81.6, 30.3]
}
Returns:
Road segments intersecting the bounding box for the given day and period.
🧱 Technical Requirements
✅ Backend
● Use FastAPI to build the microservice
● Use SQLAlchemy ORM to define data models and perform queries
● Use PostgreSQL + PostGIS for spatial data storage and filtering
● Define ORM models for:
○ Link: with spatial geometry (LINESTRING)
○ SpeedRecord: with timestamp, speed, and foreign key to Link
📘 Deliverables
Please provide the following:
1. 📁 Codebase
● FastAPI app (main.py)
● SQLAlchemy ORM models
● Database setup scripts
● Jupyter Notebook (see below)
● Folder structure following microservice best practices
2. 📓 Jupyter Notebook (Mapbox)
● Use requests to call your API
● Visualize road segments using mapboxgl
● Color segments based on average_speed
🗺 Mapbox Token:
You can obtain a free Mapbox access token at https://mapbox.com
📓 Jupyter Notebook Structure
# 1. Install Dependencies (if needed)
!pip install requests pandas geopandas mapboxgl shapely
# 2. Setup
MAPBOX_TOKEN = "your_mapbox_token_here"
BASE_URL = "http://localhost:8000"
# 3. Request Aggregated Data
params = {
"day": "Monday",
"period": "AM Peak"
}
response = requests.get(f"{BASE_URL}/aggregates/", params=params)
geojson_data = response.json()
# 4. Visualize in Mapbox
from mapboxgl.viz import ChoroplethViz
from mapboxgl.utils import create_color_stops
features = [
{
"type": "Feature",
"geometry": f["geometry"],
"properties": {
"average_speed": f["average_speed"],
"road_name": f["road_name"]
}
} for f in geojson_data
]
viz = ChoroplethViz(
{
"type": "FeatureCollection",
"features": features
},
access_token=MAPBOX_TOKEN,
color_property="average_speed",
color_stops=create_color_stops([10, 20, 30, 40, 50],
colors="Reds"),
center=(-81.6557, 30.3322),
zoom=11,
line_width=1.5,
opacity=0.8,
legend_title="Average Speed (mph)"
)
viz.show()
# 5. Optional: Tabular Summary
import pandas as pd
df = pd.DataFrame([
{
"link_id": f["link_id"],
"avg_speed": f["average_speed"],
"road_name": f["road_name"],
"length": f["length"]
} for f in geojson_data
])
df.sort_values("avg_speed").head(10)
3. 🏗 Architecture Diagram

## System Architecture

### High-Level Architecture Overview

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Data Sources  │    │  Data Ingestion  │    │    Database     │
│                 │    │                  │    │                 │
│ • Speed Records │───▶│ • load_speed_    │───▶│  PostgreSQL +   │
│   PARQUET       │    │   records.py     │    │    PostGIS      │
│ • Link Info     │    │ • load_link.py   │    │                 │
│   PARQUET       │    │                  │    │ • speed_record  │
└─────────────────┘    └──────────────────┘    │ • link          │
                                               └─────────────────┘
                                                          │
                                                          ▼
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│    Clients      │    │   FastAPI App    │    │   Services      │
│                 │    │                  │    │                 │
│ • Web Apps      │◀───│ • Routers        │◀───│ • aggregate.py  │
│ • Mobile Apps   │    │   - aggregate    │    │ • Business      │
│ • API Users     │    │   - aggregate_   │    │   Logic         │
│ • Notebooks     │    │     link         │    │ • Spatial       │
│                 │    │   - spatial_     │    │   Queries       │
└─────────────────┘    │     filter       │    └─────────────────┘
                       │ • Models         │
                       │ • Validation     │
                       └──────────────────┘
```

### Detailed Component Architecture

For a comprehensive architecture diagram and detailed component documentation, see [ARCHITECTURE.md](ARCHITECTURE.md).

### Key Features

- **Spatial Database**: PostgreSQL with PostGIS for geometric operations
- **Async API**: FastAPI with async/await for high performance
- **Data Validation**: Pydantic models for request/response validation
- **Modular Design**: Clear separation of concerns (routing, business logic, data access)
- **Comprehensive Testing**: Full test suite with 55+ test cases
- **Analytics Ready**: Direct database access for Jupyter notebooks

● Format: Mermaid diagram embedded in [ARCHITECTURE.md](ARCHITECTURE.md)
