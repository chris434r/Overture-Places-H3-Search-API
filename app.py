import json
import os  # For accessing environment variables
from collections import Counter, OrderedDict

import h3  # H3 library for hex-based spatial indexing
import psycopg2  # For connecting to a PostgreSQL database
from flask import Flask, jsonify, make_response  # Flask application and JSON response handling
from flask.views import MethodView  # For creating class-based views in Flask
from flask_smorest import Api, Blueprint  # For API management and Swagger documentation with blueprints

application = Flask(__name__)

# Configuration class for the API
class APIConfig:
    # Define metadata for the API documentation
    API_TITLE = "Overture Places H3 Search API"
    API_VERSION = "V1"
    OPENAPI_VERSION = "3.1.0"  # OpenAPI version for Swagger documentation
    OPENAPI_URL_PREFIX = "/"  # Base URL for the API
    OPENAPI_SWAGGER_UI_PATH = "/docs"  # Path for Swagger UI
    OPENAPI_SWAGGER_UI_URL = "https://cdn.jsdelivr.net/npm/swagger-ui-dist/"  # CDN link for Swagger UI files


# Load configuration settings into the Flask application
application.config.from_object(APIConfig)

# Initialize application
api = Api(application)

# Blueprint definition

h3_search = Blueprint(
    "h3_search",  # Internal reference name in snake_case
    __name__,  # Import name, often matches the module name
    url_prefix="/h3",  # URL prefix applied to all routes
    description=
    "Query the overture maps places dataset by providing a H3 Index (London only)" # Human-readable description for Swagger UI
)

def get_db_connection():
    # Uses environment variables to securely fetch PostgreSQL connection details
    return psycopg2.connect(
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        sslmode=os.getenv("SSL_MODE")
    )




# Define the HexID class-based view to handle H3 hex ID generation requests
@h3_search.route("/<lat>/<long>/<res>",
                               methods=["GET"])  # Define the route with lat, lng, and res as URL parameters
class H3Index (MethodView):

    @h3_search.response(200)  # Specify expected 200 OK response in Swagger documentation
    def get(self, lat, long, res):
        """
        Returns the H3 Index for a given latitude, longitude, and resolution.
        """
        try:
            # Convert lat, lng, and resolution to their appropriate types
            float_lat = float(lat)
            float_long = float(long)
            int_res = int(res)

            # Use H3 library to convert lat, lng, and res to a hex ID
            h3_index = h3.latlng_to_cell(float_lat, float_long, int_res)

            # Return the hex ID in JSON format with a 200 status code
            return jsonify({
                'h3_index': h3_index,
                "resolution": int_res,
            })
        except Exception as e:
            # If there's an error (e.g., invalid input), return a 400 status with an error message
            return jsonify({'status_code': 400, 'error': str(e)}), 400


@h3_search.route("/overture_places/<h3_index>",
                               methods=["GET"])  # Define the route with h3_index as a URL parameter
class OverturePlaces(MethodView):

    @h3_search.response(200)  # Specify expected 200 OK response in Swagger documentation
    def get(self, h3_index):
        """
        Returns overture places within the specified H3 index (resolutions 7–10) in Greater London.
        """
        resolution = h3.get_resolution(h3_index)
        # Establish a database connection
        conn = get_db_connection()

        try:
            # Use a context manager to execute the query and fetch results
            with conn:
                with conn.cursor() as curs:
                    curs.execute(
                        """
                        SELECT primary_name, primary_category, secondary_category, websites, lat, long
                        FROM public.london_poi_overture_hex
                        WHERE h3index_lv%s = %s
                        """,
                        (resolution, h3_index,),
                    )
                    rows = curs.fetchall()

                    # Process rows into a list of dictionaries
                    overture_places = [
                        {
                            "primary_name": row[0],
                            "primary_category": row[1],
                            "secondary_category": row[2],
                            "website": row[3],
                            "lat": row[4],
                            "long": row[5],
                        }
                        for row in rows
                    ]
                    feature_count = len(overture_places)

                    if overture_places:  # Simplified check for non-empty list
                        # Extract the 'secondary_category' directly from the dictionaries
                        secondary_category = [place["secondary_category"] for place in overture_places if
                                              "secondary_category" in place]

                        # Count the occurrences of each item
                        value_counter = Counter(secondary_category)

                        # Get the most common item and its count
                        secondary_category_mode, count = value_counter.most_common(1)[0]

                    # Constructing the response with an explicit order
                    response_data = OrderedDict([
                        ('h3_index', h3_index),
                        ('resolution', resolution),
                        ('total_feature_count', feature_count),
                        ('most_frequent_secondary_category', {
                            'category': secondary_category_mode,
                            'count': count,
                        }),
                        ('result', overture_places),
                    ])

                    response = make_response(json.dumps(response_data))
                    response.headers['Content-Type'] = 'application/json'
                    return response
        except Exception as e:
            # If there's an error (e.g., database connection issue), return a 500 status with an error message
            return jsonify({'status_code': 500, 'error': str(e)}), 500
        finally:
            # Ensure the database connection is closed after the request is completed
            conn.close()



api.register_blueprint(h3_search)